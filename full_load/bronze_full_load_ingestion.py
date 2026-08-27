# Databricks notebook source
# =====================================================
# CELL 1 - CONFIGURATION & PARAMETERS
# =====================================================
#
# Purpose:
#   Initializes runtime configuration for the
#   Bronze Ingestion Pipeline.
#
# Responsibilities:
#   - Import required libraries
#   - Load notebook parameters
#   - Configure Spark settings
#   - Define scheduler configuration
#   - Define audit configuration
#   - Generate execution identifier
#
# Notes:
#   No metadata loading or ingestion execution
#   occurs in this cell.
#
# =====================================================

"""
Bronze Ingestion Pipeline

Purpose:
    Execute Bronze ingestion using registry-based
    orchestration.

Key Features:
    - Registry driven ingestion
    - Dynamic worker allocation
    - Replay mode support
    - Audit logging
    - Watermark tracking
    - Tenant isolation

Execution Flow:

    Metadata Load
          |
          v
    Batch Discovery
          |
          v
    Scheduler
          |
          v
    Audit Logging
          |
          v
    Watermark Update
          |
          v
    Pipeline Summary

Version:
    1.0
"""

# =====================================================
# IMPORTS
# =====================================================

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import builtins
from pyspark.sql import Row

from datetime import datetime

import uuid
import time
import traceback
import re
from decimal import Decimal

# =====================================================
# NOTEBOOK PARAMETERS
# =====================================================

dbutils.widgets.text("run_mode", "NORMAL")
dbutils.widgets.text("tenant_filter", "")
dbutils.widgets.text("replay_from_batch", "")
dbutils.widgets.text("replay_to_batch", "")

run_mode = dbutils.widgets.get("run_mode").strip().upper()

tenant_filter = dbutils.widgets.get("tenant_filter").strip()

replay_from_batch = dbutils.widgets.get("replay_from_batch").strip()

replay_to_batch = dbutils.widgets.get("replay_to_batch").strip()

assert run_mode in ("NORMAL", "REPLAY")
# =====================================================
# CONTROL TABLES
# =====================================================

BRONZE_TABLE_REGISTRY = "control.bronze_table_registry"

BRONZE_TABLE_WATERMARK = "control.bronze_table_watermark"

BRONZE_BATCH_WATERMARK = "control.bronze_batch_watermark"

PIPELINE_EXECUTION_TABLE = "control.bronze_pipeline_execution"

PIPELINE_EXECUTION_DETAIL_TABLE = "control.bronze_pipeline_execution_detail"
# =====================================================
# SPARK CONFIGURATION
# =====================================================

spark.sql("USE CATALOG medallion")

spark.conf.set("spark.sql.iceberg.schema.autoMerge.enabled", "true")

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

spark.conf.set("spark.databricks.delta.retryWriteConflict.enabled", "true")

# =====================================================
# INGESTION CONFIGURATION
# =====================================================

LANDING_BASE_PATH = "abfss://landing@eus1pdatpltstg01.dfs.core.windows.net/fullload"

# =====================================================
# SCHEDULER CONFIGURATION
# =====================================================

BASE_WORKERS = 8

MIN_WORKERS = 2

MAX_WORKERS_LIMIT = 8

CHUNK_SIZE = 10

MAX_RETRIES = 3

RETRY_WAIT = 5

PROGRESS_INTERVAL = 50

current_workers = BASE_WORKERS


# =====================================================
# EXECUTION CONTEXT
# =====================================================

PIPELINE_VERSION = "1.0"

EXECUTION_ID = datetime.now().strftime("%Y%m%d%H%M%S%f")

PIPELINE_START_TS = datetime.now()

print("=" * 80)
print("BRONZE PIPELINE STARTED")
print("=" * 80)

print(f"Version      : {PIPELINE_VERSION}")

print(f"Execution Id : {EXECUTION_ID}")

print(f"Start Time   : {PIPELINE_START_TS}")

print(f"Run Mode     : {run_mode}")

print(f"Base Workers : {BASE_WORKERS}")

print(f"Max Workers  : {MAX_WORKERS_LIMIT}")

print("=" * 80)

# COMMAND ----------

# =====================================================
# CELL 2 - METADATA LOADING & BATCH DISCOVERY
# =====================================================
#
# Purpose:
#   Loads Bronze ingestion metadata required
#   for execution scheduling.
#
# Responsibilities:
#   - Load active Bronze tables
#   - Discover active tenants
#   - Discover pending batches
#   - Build execution batch map
#
# Output:
#   all_tables
#   all_tenants
#   batch_map
#
# Notes:
#   No ingestion execution occurs in this cell.
#
# =====================================================

# =====================================================
# LOAD ACTIVE TABLES
# =====================================================


def load_active_tables():
    """
    Loads active Bronze tables from registry.

    Returns:
        list[Row]
    """

    return spark.sql(f"""
            SELECT
                source_schema,
                source_table,
                bronze_table,
                pk_columns,
                batch_group,
                topic_group,
                tenant_group
            FROM {BRONZE_TABLE_REGISTRY}
            WHERE is_active = true
            ORDER BY source_schema,
                     source_table
        """).collect()


# =====================================================
# DISCOVER TENANTS
# =====================================================


def discover_tenants():
    """
    Discovers active tenants.

    Returns:
        list[int]
    """

    if tenant_filter:
        return sorted([int(x.strip()) for x in tenant_filter.split(",") if x.strip()])

    tenants = []

    try:
        for f in dbutils.fs.ls(LANDING_BASE_PATH):
            if f.name.startswith("tenant_id="):
                tenants.append(int(f.name.replace("tenant_id=", "").rstrip("/")))

    except Exception as ex:
        raise Exception(f"Tenant discovery failed: {str(ex)}")

    return sorted(tenants)


# =====================================================
# LOAD WATERMARK
# =====================================================


def get_last_batch_id(tenant_id, schema_name, source_table):
    return watermark_map.get((tenant_id, schema_name, source_table))


# =====================================================
# DISCOVER TABLE BATCHES
# =====================================================


def list_batches_for_table(tenant_id, schema_name, source_table):
    """
    Returns available landing batches.
    """

    path = (
        f"{LANDING_BASE_PATH}"
        f"/tenant_id={tenant_id}"
        f"/schema_name={schema_name}"
        f"/table_name={source_table}"
    )

    batches = []

    try:
        for f in dbutils.fs.ls(path):
            if f.name.startswith("batch_id="):
                batches.append(int(f.name.replace("batch_id=", "").rstrip("/")))

    except Exception:
        pass

    return sorted(batches)


# =====================================================
# APPLY REPLAY FILTER
# =====================================================


def apply_replay_range(batches):
    """
    Filters batches for replay mode.
    """

    return [
        b
        for b in batches
        if (not replay_from_batch or b >= int(replay_from_batch))
        and (not replay_to_batch or b <= int(replay_to_batch))
    ]


# =====================================================
# LOAD METADATA
# =====================================================

all_tables = load_active_tables()

if not all_tables:
    raise Exception("No active Bronze tables found.")

all_tenants = discover_tenants()
# =====================================================
# LOAD WATERMARKS ONCE
# =====================================================

watermark_rows = spark.sql(f"""
    SELECT
        tenant_id,
        schema_name,
        source_table,
        last_batch_id
    FROM {BRONZE_TABLE_WATERMARK}
""").collect()

watermark_map = {
    (row["tenant_id"], row["schema_name"], row["source_table"]): row["last_batch_id"]
    for row in watermark_rows
}

if not all_tenants:
    raise Exception("No tenants discovered.")

# =====================================================
# METADATA SUMMARY
# =====================================================

print("=" * 80)
print("METADATA SUMMARY")
print("=" * 80)

print(f"Active Tables      : {len(all_tables)}")

print(f"Active Tenants     : {len(all_tenants)}")

print(f"Run Mode           : {run_mode}")

print("=" * 80)

# COMMAND ----------

# =====================================================
# CELL 3 - EXECUTION FUNCTIONS
# =====================================================
#
# Purpose:
#   Contains all Bronze ingestion execution logic.
#
# Responsibilities:
#   - Build audit records
#   - Retry wrappers
#   - Delete for replay mode
#   - Append/Merge Bronze data
#   - Process tenant/table batches
#
# Output:
#   process_table()
#
# Notes:
#   No scheduler logic occurs in this cell.
#
# =====================================================

# =====================================================
# GLOBAL EXECUTION TRACKING
# =====================================================

audit_rows = []

failed_tables = []

success_updates = []


def chunk_list(items, chunk_size):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


# =====================================================
# AUDIT ROW BUILDER
# =====================================================


def build_audit_row(
    tenant_id,
    schema_name,
    source_table,
    batch_id,
    status,
    inserted_rows=0,
    error_message=None,
    start_ts=None,
    end_ts=None,
):
    """
    Creates Bronze execution detail row.
    """

    duration_secs = None

    if start_ts and end_ts:
        duration_secs = int((end_ts - start_ts).total_seconds())

    return Row(
        execution_id=EXECUTION_ID,
        tenant_id=tenant_id,
        schema_name=schema_name,
        source_table=source_table,
        batch_id=batch_id,
        status=status,
        inserted_rows=inserted_rows,
        error_message=error_message,
        start_ts=start_ts,
        end_ts=end_ts,
        duration_secs=duration_secs,
        created_ts=datetime.now(),
    )


# =====================================================
# RETRY WRAPPERS
# =====================================================


def safe_delete(query):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            spark.sql(query)

            return

        except Exception as ex:
            if attempt == MAX_RETRIES:
                raise

            print(f"⚠️ DELETE Retry {attempt}/{MAX_RETRIES}")

            time.sleep(RETRY_WAIT)


def safe_append(df, table_name):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            (df.writeTo(table_name).append())

            return

        except Exception as ex:
            if attempt == MAX_RETRIES:
                raise

            print(f"⚠️ APPEND Retry {attempt}/{MAX_RETRIES}")

            time.sleep(RETRY_WAIT)


def safe_merge(df, table_name, pk_cols):
    """
    Performs MERGE with retry support for Delta
    concurrency conflicts.
    """

    concurrency_errors = (
        "ConcurrentAppendException",
        "DELTA_CONCURRENT_APPEND",
        "ConcurrentTransactionException",
    )

    for attempt in range(1, MAX_RETRIES + 1):
        view_name = None

        try:
            safe_table_name = re.sub(r"[^A-Za-z0-9_]", "_", table_name)

            view_name = f"source_data_{safe_table_name}_{int(time.time() * 1000)}"

            df.createOrReplaceTempView(view_name)

            merge_condition = " AND ".join([f"t.`{c}` = s.`{c}`" for c in pk_cols])

            update_set = ", ".join([f"t.`{c}` = s.`{c}`" for c in df.columns])

            insert_cols = ", ".join([f"`{c}`" for c in df.columns])

            insert_vals = ", ".join([f"s.`{c}`" for c in df.columns])

            merge_sql = f"""
                MERGE INTO {table_name} t
                USING {view_name} s
                ON {merge_condition}

                WHEN MATCHED THEN
                UPDATE SET
                    {update_set}

                WHEN NOT MATCHED THEN
                INSERT (
                    {insert_cols}
                )
                VALUES (
                    {insert_vals}
                )
            """

            spark.sql(merge_sql)
            if attempt > 1:
                print(
                    f"✅ MERGE succeeded after retry "
                    f"| Table={table_name} "
                    f"| Attempt={attempt}/{MAX_RETRIES}"
                )
            return

        except Exception as ex:
            error_message = str(ex)

            # ---------------------------------------------
            # Retry only Delta concurrency conflicts
            # ---------------------------------------------

            if any(err in error_message for err in concurrency_errors):
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_WAIT * (2 ** (attempt - 1))

                    print(
                        f"⚠️ Concurrent MERGE conflict "
                        f"| Attempt {attempt}/{MAX_RETRIES} "
                        f"| Waiting {wait_time}s"
                    )

                    time.sleep(wait_time)

                    continue

            # Any other error OR retries exhausted
            raise

        finally:
            if view_name:
                try:
                    spark.catalog.dropTempView(view_name)
                except:
                    pass


# =====================================================
# PROCESS TABLE
# =====================================================


def process_table(tenant_id, table_row):
    """
    Processes all pending batches for a
    tenant/schema/table combination.
    """

    schema_name = table_row["source_schema"]

    source_table = table_row["source_table"]

    bronze_table = table_row["bronze_table"]

    bronze_fqn = f"medallion.bronze.{bronze_table}"

    pk_columns_raw = table_row["pk_columns"]

    if pk_columns_raw:
        pk_cols = [c.strip() for c in pk_columns_raw.split(",")]

        pk_cols_with_tenant = ["tenant_id"] + pk_cols

    else:
        pk_cols = None

        pk_cols_with_tenant = None

    result = {
        "tenant_id": tenant_id,
        "schema_name": schema_name,
        "source_table": source_table,
        "status": "SUCCESS",
        "last_success_batch_id": None,
        "audit_rows": [],
        "failures": [],
    }

    try:
        # ==========================================
        # DISCOVER AVAILABLE BATCHES
        # ==========================================

        batches = list_batches_for_table(tenant_id, schema_name, source_table)

        # ==========================================
        # APPLY REPLAY / INCREMENTAL FILTER
        # ==========================================

        if run_mode == "REPLAY":
            batches = apply_replay_range(batches)

        else:
            last_batch = watermark_map.get((tenant_id, schema_name, source_table))

            if last_batch:
                batches = [b for b in batches if b > last_batch]

        # ==========================================
        # NO PENDING BATCHES
        # ==========================================

        if not batches:
            result["status"] = "SKIPPED"

            result["audit_rows"].append(
                build_audit_row(
                    tenant_id=tenant_id,
                    schema_name=schema_name,
                    source_table=source_table,
                    batch_id=None,
                    status="SKIPPED",
                )
            )

            return result

        # ==========================================
        # PROCESS BATCHES
        # ==========================================

        for batch_id in batches:
            batch_start_ts = datetime.now()

            try:
                source_path = (
                    f"{LANDING_BASE_PATH}"
                    f"/tenant_id={tenant_id}"
                    f"/schema_name={schema_name}"
                    f"/table_name={source_table}"
                    f"/batch_id={batch_id}"
                )

                df = spark.read.parquet(source_path)

                source_rows = df.count()

                df_final = (
                    df.withColumn("tenant_id", lit(tenant_id))
                    .withColumn("batch_id", lit(batch_id))
                    .withColumn("_ingest_date", current_date())
                    .withColumn("_op", lit("I"))
                )

                if pk_cols_with_tenant:
                    df_final = df_final.dropDuplicates(pk_cols_with_tenant)

                if run_mode == "REPLAY":
                    safe_delete(f"""
                        DELETE FROM {bronze_fqn}
                        WHERE tenant_id = {tenant_id}
                          AND batch_id = {batch_id}
                    """)

                if pk_cols_with_tenant:
                    safe_merge(df_final, bronze_fqn, pk_cols_with_tenant)

                else:
                    safe_append(df_final, bronze_fqn)

                result["last_success_batch_id"] = batch_id

                result["audit_rows"].append(
                    build_audit_row(
                        tenant_id=tenant_id,
                        schema_name=schema_name,
                        source_table=source_table,
                        batch_id=batch_id,
                        status="SUCCESS",
                        inserted_rows=source_rows,
                        start_ts=batch_start_ts,
                        end_ts=datetime.now(),
                    )
                )

            except Exception as ex:
                error_message = str(ex)

                result["status"] = "PARTIAL_SUCCESS"

                result["failures"].append(error_message)

                result["audit_rows"].append(
                    build_audit_row(
                        tenant_id=tenant_id,
                        schema_name=schema_name,
                        source_table=source_table,
                        batch_id=batch_id,
                        status="FAILED",
                        inserted_rows=0,
                        error_message=error_message,
                        start_ts=batch_start_ts,
                        end_ts=datetime.now(),
                    )
                )

        return result

    except Exception as ex:
        result["status"] = "FAILED"

        result["failures"].append(str(ex))

        return result

# COMMAND ----------

# =====================================================
# CELL 4 - SCHEDULER & EXECUTION ORCHESTRATION
# =====================================================
#
# Purpose:
#   Executes Bronze ingestion using
#   tenant-aware parallel scheduling.
#
# Responsibilities:
#   - Execute table ingestion
#   - Collect audit rows
#   - Track failures
#   - Track watermark updates
#   - Apply smart throttling
#
# Output:
#   audit_rows
#   success_updates
#   failed_tables
#
# =====================================================

table_chunks = list(chunk_list(all_tables, CHUNK_SIZE))

chunk_counter = 1

skipped_count = 0

successful_count = 0

failed_count = 0

print("=" * 80)
print("PIPELINE EXECUTION STARTED")
print("=" * 80)

# =====================================================
# TABLE WRITE LOCKS
# =====================================================

active_table_locks = set()

for chunk in table_chunks:
    print(f"\n🔥 Chunk {chunk_counter} | Workers={current_workers}")

    chunk_failures = 0

    total_tasks = 0

    # ==========================================
    # BUILD PENDING TASKS
    # ==========================================

    pending_tasks = [
        (tenant_id, table_row) for tenant_id in all_tenants for table_row in chunk
    ]

    with ThreadPoolExecutor(max_workers=current_workers) as executor:
        running_futures = {}

        while pending_tasks or running_futures:
            # =====================================
            # SUBMIT NEW TASKS
            # =====================================

            while len(running_futures) < current_workers and pending_tasks:
                submitted = False

                for i, (tenant_id, table_row) in enumerate(pending_tasks):
                    table_name = table_row["source_table"]

                    # ---------------------------------
                    # SAME TABLE ALREADY RUNNING
                    # ---------------------------------

                    if table_name in active_table_locks:
                        continue

                    future = executor.submit(process_table, tenant_id, table_row)

                    running_futures[future] = (tenant_id, table_row)

                    active_table_locks.add(table_name)

                    pending_tasks.pop(i)

                    total_tasks += 1

                    submitted = True

                    break

                # ---------------------------------
                # NO ELIGIBLE TASK FOUND
                # ---------------------------------

                if not submitted:
                    break

            # =====================================
            # NOTHING RUNNING
            # =====================================

            if not running_futures:
                time.sleep(1)
                continue

            # =====================================
            # WAIT FOR NEXT COMPLETED TASK
            # =====================================

            completed_future = next(as_completed(running_futures))

            tenant_id, table_row = running_futures.pop(completed_future)

            table_name = table_row["source_table"]

            active_table_locks.discard(table_name)

            result = completed_future.result()

            # =====================================
            # AUDIT COLLECTION
            # =====================================

            audit_rows.extend(result["audit_rows"])

            # =====================================
            # SUCCESSFUL WATERMARKS
            # =====================================

            if result["last_success_batch_id"] is not None:
                success_updates.append(
                    (
                        result["tenant_id"],
                        result["schema_name"],
                        result["source_table"],
                        result["last_success_batch_id"],
                    )
                )

            # =====================================
            # STATUS TRACKING
            # =====================================

            if result["status"] == "SUCCESS":
                successful_count += 1

            elif result["status"] == "SKIPPED":
                skipped_count += 1

            else:
                failed_count += 1

                chunk_failures += 1

                failed_tables.append(
                    (
                        result["tenant_id"],
                        result["schema_name"],
                        result["source_table"],
                        result["failures"],
                    )
                )

    # ==========================================
    # SMART THROTTLING
    # ==========================================

    failure_ratio = chunk_failures / total_tasks if total_tasks else 0

    if failure_ratio > 0.30:
        current_workers = builtins.max(MIN_WORKERS, current_workers - 1)

        print(f"⚠️ High failure rate → Workers={current_workers}")

    elif failure_ratio == 0:
        current_workers = builtins.min(MAX_WORKERS_LIMIT, current_workers + 1)

        print(f"🚀 No failures → Workers={current_workers}")

    else:
        print(f"⚖️ Stable load → Workers={current_workers}")

    print(f"Chunk Summary | Tasks={total_tasks} | Failures={chunk_failures}")

    chunk_counter += 1

print("=" * 80)
print("PIPELINE EXECUTION COMPLETED")
print("=" * 80)

print(f"Successful : {successful_count}")

print(f"Failed     : {failed_count}")

print(f"Skipped    : {skipped_count}")

print(f"Audit Rows : {len(audit_rows)}")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Cell 5
# =====================================================
# CELL 5 - WATERMARKS & PIPELINE SUMMARY
# =====================================================
#
# Purpose:
#   Finalizes Bronze execution results.
#
# Responsibilities:
#   - Update table watermarks
#   - Update bronze batch watermark
#   - Calculate pipeline metrics
#   - Build summary record
#
# Notes:
#   Audit persistence occurs in Cell 6.
#
# =====================================================

# =====================================================
# TABLE WATERMARK UPDATE
# =====================================================


def update_watermarks(success_updates):
    if not success_updates:
        print("⚠️ No successful updates found.")
        return

    df_updates = spark.createDataFrame(
        [
            Row(tenant_id=t[0], schema_name=t[1], source_table=t[2], last_batch_id=t[3])
            for t in success_updates
        ]
    )

    df_updates.createOrReplaceTempView("vw_bronze_watermark_updates")

    spark.sql(f"""
        MERGE INTO {BRONZE_TABLE_WATERMARK} t
        USING vw_bronze_watermark_updates s

        ON t.tenant_id = s.tenant_id
        AND t.schema_name = s.schema_name
        AND t.source_table = s.source_table

        WHEN MATCHED THEN
        UPDATE SET
            t.last_batch_id = s.last_batch_id,
            t.updated_on = current_timestamp()

        WHEN NOT MATCHED THEN
        INSERT
        (
            tenant_id,
            schema_name,
            source_table,
            last_batch_id,
            updated_on
        )
        VALUES
        (
            s.tenant_id,
            s.schema_name,
            s.source_table,
            s.last_batch_id,
            current_timestamp()
        )
    """)

    print(f"✅ Table Watermarks Updated : {len(success_updates)}")


# =====================================================
# DETAIL AUDIT SCHEMA
# =====================================================

detail_schema = StructType(
    [
        StructField("execution_id", StringType(), True),
        StructField("tenant_id", IntegerType(), True),
        StructField("schema_name", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("batch_id", LongType(), True),
        StructField("status", StringType(), True),
        StructField("inserted_rows", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("duration_secs", LongType(), True),
        StructField("created_ts", TimestampType(), True),
    ]
)

# =====================================================
# BRONZE BATCH WATERMARK
# =====================================================


def update_bronze_batch_watermark():
    """
    Promotes Bronze batches that completed successfully
    during the current execution.

    Rules:
      - Promote batch only if NO table failed for that batch.
      - Tables with no data do not block promotion.
      - Only promote batches greater than the last promoted batch.
    """

    # ==========================================
    # BUILD AUDIT DATAFRAME
    # ==========================================

    audit_df = spark.createDataFrame(audit_rows, schema=detail_schema)

    # Ignore SKIPPED rows (batch_id=None)
    audit_df = audit_df.filter(F.col("batch_id").isNotNull())

    if not audit_df.head(1):
        print("ℹ️ No processed batches found.")

        return

    # ==========================================
    # LAST PROMOTED BATCH
    # ==========================================

    last_promoted_batch = spark.sql(f"""
        SELECT MAX(batch_id) AS batch_id
        FROM {BRONZE_BATCH_WATERMARK}
    """).first()["batch_id"]

    last_promoted_batch = last_promoted_batch if last_promoted_batch is not None else -1

    # ==========================================
    # IDENTIFY PROMOTABLE BATCHES
    # ==========================================

    promotable_batches = (
        audit_df.groupBy("batch_id")
        .agg(
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias(
                "failed_count"
            )
        )
        .filter(
            (F.col("failed_count") == 0) & (F.col("batch_id") > last_promoted_batch)
        )
        .select("batch_id")
        .orderBy("batch_id")
    )

    if not promotable_batches.head(1):
        print("ℹ️ No new Bronze batches available for promotion.")

        return

    # ==========================================
    # PREPARE WATERMARK DATA
    # ==========================================

    watermark_df = promotable_batches.withColumn(
        "completed_ts", F.current_timestamp()
    ).withColumn("execution_id", F.lit(EXECUTION_ID))

    watermark_df.createOrReplaceTempView("vw_bronze_batch_watermark_updates")

    # ==========================================
    # INSERT NEW BATCHES
    # ==========================================

    spark.sql(f"""
        MERGE INTO {BRONZE_BATCH_WATERMARK} t
        USING vw_bronze_batch_watermark_updates s

        ON t.batch_id = s.batch_id

        WHEN NOT MATCHED THEN
        INSERT
        (
            batch_id,
            completed_ts,
            execution_id
        )
        VALUES
        (
            s.batch_id,
            s.completed_ts,
            s.execution_id
        )
    """)

    promoted_count = watermark_df.count()

    print(f"✅ Bronze Batch Watermark Updated | Promoted={promoted_count}")


# =====================================================
# EXECUTE WATERMARK UPDATES
# =====================================================

update_watermarks(success_updates)

update_bronze_batch_watermark()

# =====================================================
# PIPELINE STATUS
# =====================================================

failed_batch_count = builtins.sum(1 for row in audit_rows if row.status == "FAILED")

successful_batch_count = builtins.sum(
    1 for row in audit_rows if row.status == "SUCCESS"
)

skipped_batch_count = builtins.sum(1 for row in audit_rows if row.status == "SKIPPED")


if failed_batch_count > 0:
    pipeline_status = "FAILED"

else:
    pipeline_status = "SUCCESS"

# =====================================================
# PIPELINE DURATION
# =====================================================

PIPELINE_END_TS = datetime.now()

pipeline_duration_seconds = builtins.int(
    (PIPELINE_END_TS - PIPELINE_START_TS).total_seconds()
)


pipeline_duration_minutes = Decimal(
    str(builtins.round(pipeline_duration_seconds / 60.0, 2))
)

# =====================================================
# PIPELINE SUMMARY RECORD
# =====================================================

summary_row = Row(
    execution_id=EXECUTION_ID,
    start_ts=PIPELINE_START_TS,
    end_ts=PIPELINE_END_TS,
    duration_secs=pipeline_duration_seconds,
    duration_mins=pipeline_duration_minutes,
    total_tables=len(all_tables),
    total_tenants=len(all_tenants),
    successful_tables=successful_count,
    failed_tables=failed_count,
    skipped_tables=skipped_count,
    successful_batches=successful_batch_count,
    failed_batches=failed_batch_count,
    skipped_batches=skipped_batch_count,
    watermark_updates=len(success_updates),
    total_inserted_rows=builtins.sum(row.inserted_rows for row in audit_rows),
    status=pipeline_status,
    pipeline_version=PIPELINE_VERSION,
    created_ts=datetime.now(),
)
# =====================================================
# PIPELINE SUMMARY
# =====================================================

print("=" * 80)
print("PIPELINE SUMMARY")
print("=" * 80)

print(f"Execution Id       : {EXECUTION_ID}")

print(f"Status             : {pipeline_status}")

print(f"Total Tables       : {len(all_tables)}")

print(f"Total Tenants      : {len(all_tenants)}")

print(f"Successful Batches : {successful_batch_count}")

print(f"Failed Batches     : {failed_batch_count}")

print(f"Skipped Batches    : {skipped_batch_count}")

print(f"Watermark Updates  : {len(success_updates)}")

print(f"Inserted Rows      : {summary_row.total_inserted_rows:,}")

print(f"Duration Seconds   : {pipeline_duration_seconds}")

print(f"Duration Minutes   : {pipeline_duration_minutes}")

print("=" * 80)

# COMMAND ----------

# =====================================================
# CELL 6 - AUDIT PERSISTENCE
# =====================================================
#
# Purpose:
#   Persists Bronze execution audit data.
#
# Responsibilities:
#   - Write pipeline summary
#   - Write pipeline detail records
#   - Display execution summary
#
# Notes:
#   Final pipeline cell.
#
# =====================================================

# =====================================================
# PIPELINE EXECUTION AUDIT
# =====================================================

execution_schema = StructType(
    [
        StructField("execution_id", StringType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("duration_secs", LongType(), True),
        StructField("duration_mins", DecimalType(18, 2), True),
        StructField("total_tables", IntegerType(), True),
        StructField("total_tenants", IntegerType(), True),
        StructField("successful_tables", IntegerType(), True),
        StructField("failed_tables", IntegerType(), True),
        StructField("skipped_tables", IntegerType(), True),
        StructField("successful_batches", IntegerType(), True),
        StructField("failed_batches", IntegerType(), True),
        StructField("skipped_batches", IntegerType(), True),
        StructField("watermark_updates", IntegerType(), True),
        StructField("total_inserted_rows", LongType(), True),
        StructField("status", StringType(), True),
        StructField("pipeline_version", StringType(), True),
        StructField("created_ts", TimestampType(), True),
    ]
)

# =====================================================
# WRITE PIPELINE EXECUTION
# =====================================================

summary_df = spark.createDataFrame([summary_row], schema=execution_schema)

(summary_df.write.mode("append").saveAsTable(PIPELINE_EXECUTION_TABLE))

print(f"✅ Pipeline Execution Written")

# =====================================================
# WRITE PIPELINE DETAIL
# =====================================================

if audit_rows:
    detail_df = spark.createDataFrame(audit_rows, schema=detail_schema)

    (detail_df.write.mode("append").saveAsTable(PIPELINE_EXECUTION_DETAIL_TABLE))

    print(f"✅ Detail Records Written : {len(audit_rows)}")

else:
    print("ℹ️ No detail records generated.")

# =====================================================
# FAILED TABLE SUMMARY
# =====================================================

if failed_tables:
    print("=" * 80)
    print("FAILED OBJECTS")
    print("=" * 80)

    for tenant_id, schema_name, source_table, failures in failed_tables:
        print(f"Tenant={tenant_id} | Schema={schema_name} | Table={source_table}")

        for failure in failures:
            print(f"    {failure}")

# =====================================================
# FINAL COMPLETION MESSAGE
# =====================================================

print("=" * 80)
print("BRONZE PIPELINE COMPLETED")
print("=" * 80)

print(f"Execution Id : {EXECUTION_ID}")

print(f"Status       : {summary_row.status}")

print(f"Duration     : {summary_row.duration_mins} mins")

print(f"Audit Rows   : {len(audit_rows)}")

print("=" * 80)