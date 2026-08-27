# Databricks notebook source
# MAGIC %md
# MAGIC # stage_compact — the MERGE engine, decoupled from ingest
# MAGIC
# MAGIC The compaction half of the stage-append + periodic-MERGE re-architecture (`cdc/KEEPUP-ARCHITECTURE.md`). Reads a
# MAGIC **boundary-claimed** slice of `medallion.fire_nfris_bronze._cdc_stage`, groups by `(tenant, schema, table)`, and for each table
# MAGIC does a **single-pass latest-op-wins** MERGE into `medallion.fire_nfris_bronze.<table>`. It has **no Event Hub offset to advance** —
# MAGIC a slow run just means fire_nfris_bronze is one cycle behind; the 2026-06-30 checkpoint stall is structurally impossible here.
# MAGIC Per-table MERGEs run concurrently (distinct Delta targets = no conflict); the stage DELETE + cursor + watermark are
# MAGIC batched ONCE after the pool (avoids concurrent-write conflicts on the shared stage/cursor tables). Single writer.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import json, uuid

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("stage_table", "medallion.fire_nfris_bronze._cdc_stage")
dbutils.widgets.text("cursor_table", "medallion.fire_nfris_control.cdc_stage_compaction_cursor")
dbutils.widgets.text("registry_table", "medallion.fire_nfris_control.bronze_table_registry")
dbutils.widgets.text("target_catalog", "medallion")
dbutils.widgets.text("target_schema", "fire_nfris_bronze")
dbutils.widgets.text("source_schema", "FireEvent,FireInvestigation,FireInvestigationLocation")
dbutils.widgets.text("tenant_id", "1")
dbutils.widgets.text("merge_max_workers", "16")
dbutils.widgets.text("deadletter_table", "medallion.fire_nfris_bronze._cdc_stream_deadletter")
# fire_nfris_control/audit tables (parameterized so a non-blue catalog run cannot write into prod fire_nfris_control tables)
dbutils.widgets.text("batch_watermark_table", "medallion.fire_nfris_control.bronze_table_watermark")
dbutils.widgets.text("table_watermark_table", "medallion.fire_nfris_control.bronze_table_watermark")
dbutils.widgets.text("execution_table", "medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution")
dbutils.widgets.text("execution_detail_table", "medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution_detail")
dbutils.widgets.text("promotion_gate_table", "medallion.fire_nfris_control.bronze_batch_watermark")
# --- sliding-window (late-arrival) fire_nfris_controls — OFF by default; green sets them, blue keeps current behavior ---
dbutils.widgets.text("grace_minutes", "0")          # hold back rows staged within this many minutes so a
                                                    #   logical unit's late-arriving events settle before compaction
dbutils.widgets.dropdown("unify_batch_id", "false", ["true", "false"])  # one batch_id for the whole settled
                                                    #   slice -> related rows ACROSS tables share a batch_id
                                                    #   (needed for Silver's per-batch join; else per-table max)

g = lambda k: dbutils.widgets.get(k).strip()
STAGE=g("stage_table"); CURSOR=g("cursor_table"); REG=g("registry_table")
TCAT=g("target_catalog"); TSCH=g("target_schema"); SRC_SCHEMA=g("source_schema"); TENANT=int(g("tenant_id"))
MERGE_WORKERS=int(g("merge_max_workers") or "16"); DLQ=g("deadletter_table")
GRACE=int(g("grace_minutes") or "0")
# UNIFY is an INVARIANT, not a toggle: one batch_id for the whole settled slice so every table (and
# every ingestion method/client sharing this run's id) lands under ONE batch_id that Silver can gate
# on. Running with per-table batch_ids orphaned any non-max table from fire_nfris_bronze_batch_watermark. Forced
# on regardless of the (now-deprecated) unify_batch_id widget.
UNIFY = True
BATCH_WM=g("batch_watermark_table"); TABLE_WM=g("table_watermark_table")
EXEC_TBL=g("execution_table"); EXEC_DETAIL=g("execution_detail_table"); PROMO_GATE=g("promotion_gate_table")
RUN_ID=str(uuid.uuid4())
run_start_ts=datetime.now(timezone.utc).replace(tzinfo=None)
print(f"compact stage={STAGE} tenant={TENANT} schema={SRC_SCHEMA} workers={MERGE_WORKERS} run_id={RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Control tables the compactor feeds (reused verbatim from eventhub_to_bronze_stream.py, feedback #4)
def write_fire_nfris_bronze_watermarks(*, success_updates):
    """Advance fire_nfris_control.batch_watermark (Silver keystone) + fire_nfris_control.fire_nfris_bronze_table_watermark, one chunked MERGE each (#7)."""
    if not success_updates:
        return
    wm = (spark.createDataFrame(success_updates,
             "tenant_id int, schema_name string, source_table string, last_batch_id bigint")
          .withColumn("updated_on", F.current_timestamp()))
    for tbl in (BATCH_WM, TABLE_WM):
        try:
            (DeltaTable.forName(spark, tbl).alias("t").merge(wm.alias("s"),
                "t.tenant_id=s.tenant_id AND t.schema_name=s.schema_name AND t.source_table=s.source_table")
                .whenMatchedUpdate(set={"last_batch_id": "s.last_batch_id", "updated_on": "s.updated_on"})
                .whenNotMatchedInsert(values={"tenant_id": "s.tenant_id", "schema_name": "s.schema_name",
                      "source_table": "s.source_table", "last_batch_id": "s.last_batch_id",
                      "updated_on": "s.updated_on"}).execute())
        except Exception as e:
            print(f"{tbl} skipped:", str(e)[:150])

def write_fire_nfris_bronze_run_audit(*, tenant_id, run_batch, run_start_ts, run_end_ts, pipeline_version,
                           detail_rows, success_updates):
    """Run-level audit: fire_nfris_control.fire_nfris_bronze_pipeline_execution(_detail) + the promotion gate fire_nfris_control.fire_nfris_bronze_batch_watermark."""
    execution_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    rb = int(run_batch); dur = int((run_end_ts - run_start_ts).total_seconds())
    created = datetime.now(timezone.utc).replace(tzinfo=None)
    n_ok = sum(1 for r in detail_rows if r["status"] == "SUCCESS")
    n_fail = sum(1 for r in detail_rows if r["status"] in ("FAILED", "FATAL"))
    n_skip = sum(1 for r in detail_rows if r["status"] == "SKIPPED")
    detail_ddl = ("execution_id string, tenant_id int, schema_name string, source_table string, "
                  "batch_id bigint, status string, inserted_rows bigint, error_message string, "
                  "start_ts timestamp, end_ts timestamp, duration_secs bigint, created_ts timestamp")
    drows = [(execution_id, int(tenant_id), r["schema_name"], r["source_table"],
              (int(r["batch_id"]) if r.get("batch_id") is not None and r["status"] != "SKIPPED" else None),
              r["status"], int(r.get("inserted_rows") or 0), (r.get("error_message") or None),
              (r.get("start_ts") or run_start_ts), (r.get("end_ts") or run_end_ts),
              (int(r["duration_secs"]) if r.get("duration_secs") is not None else dur), created) for r in detail_rows]
    try:
        if drows:
            spark.createDataFrame(drows, schema=detail_ddl).write.mode("append").saveAsTable(
                EXEC_DETAIL)
    except Exception as e:
        print("fire_nfris_bronze_pipeline_execution_detail skipped:", str(e)[:150])
    status = "FAILED" if n_fail else "SUCCESS"
    exec_ddl = ("execution_id string, start_ts timestamp, end_ts timestamp, duration_secs bigint, "
                "duration_mins decimal(18,2), total_tables int, total_tenants int, successful_tables int, "
                "failed_tables int, skipped_tables int, successful_batches int, failed_batches int, "
                "skipped_batches int, watermark_updates int, total_inserted_rows bigint, status string, "
                "pipeline_version string, created_ts timestamp")
    erow = (execution_id, run_start_ts, run_end_ts, dur, Decimal(str(round(dur / 60.0, 2))),
            len(detail_rows), 1, n_ok, n_fail, n_skip, n_ok, n_fail, n_skip, len(success_updates),
            sum(int(r.get("inserted_rows") or 0) for r in detail_rows), status, pipeline_version, created)
    try:
        spark.createDataFrame([erow], schema=exec_ddl).write.mode("append").saveAsTable(
            EXEC_TBL)
    except Exception as e:
        print("fire_nfris_bronze_pipeline_execution skipped:", str(e)[:150])
    try:
        if n_fail == 0 and success_updates:
            # Register this run's batch_id in the promotion gate UNCONDITIONALLY. The old `rb > max`
            # guard only inserted a strictly-increasing batch_id, so any run whose batch_id was not the
            # newest (out-of-order completion, or a smaller per-client/per-method id) left its fire_nfris_bronze
            # rows with NO watermark row -> Silver never processed them (orphaned batch). The MERGE on
            # batch_id already dedupes, so re-registering an existing id is a no-op.
            bw = spark.createDataFrame([(rb, created, execution_id)],
                    "batch_id bigint, completed_ts timestamp, execution_id string")
            (DeltaTable.forName(spark, PROMO_GATE).alias("t")
                .merge(bw.alias("s"), "t.batch_id=s.batch_id")
                .whenNotMatchedInsert(values={"batch_id": "s.batch_id", "completed_ts": "s.completed_ts",
                      "execution_id": "s.execution_id"}).execute())
    except Exception as e:
        print("fire_nfris_bronze_batch_watermark skipped:", str(e)[:150])
    return execution_id

# COMMAND ----------

# DBTITLE 1,Registry snapshot + per-table business schema + DLQ (reused from the stream)
META_COLS={"tenant_id","_op","_ingest_date","ingest_date","batch_id","run_id","source_log_position",
           "ingestion_time","ingest_ts","after_json","debezium_ts_ms","eh_enqueued_time","commit_lsn",
           "operation_type","source_db","source_schema","source_table","_deleted","__deleted"}
# source_schema accepts a CSV of schemas, or "*" for every registered schema. Debezium captures whatever
# has CDC enabled at the source (20 schemas for a full client), so scoping the consumer to a single schema
# left the rest stranded in the stage. is_active is re-read here on EVERY run — it is the runtime landing
# gate, deliberately NOT baked into the connector config.
SRC_SCHEMAS = None if SRC_SCHEMA.strip() == "*" else {
    s.strip().lower() for s in SRC_SCHEMA.split(",") if s.strip()}
_reg = spark.table(REG).filter("is_active = true")
if SRC_SCHEMAS:
    _reg = _reg.filter(F.lower(F.trim("source_schema")).isin(list(SRC_SCHEMAS)))
reg_rows = _reg.select("source_schema", "source_table", "bronze_table", "pk_columns").collect()
# keyed by (schema, table): a name-only key collides across schemas (incident/patient/fieldvalue and 7
# more exist in 2-3 schemas) and would resolve to the wrong fire_nfris_bronze table.
REGISTRY = {}
for r in reg_rows:
    pk = [c.strip() for c in (r["pk_columns"] or "").split(",") if c.strip()]
    if r["bronze_table"] and pk:
        REGISTRY[((r["source_schema"] or "").strip().lower(), r["source_table"].lower())] = \
            {"bronze_table": r["bronze_table"], "pk": pk}
_scope = "ALL schemas" if SRC_SCHEMAS is None else ",".join(sorted(SRC_SCHEMAS))
print(f"active registered tables with PK [{_scope}]: {len(REGISTRY)} "
      f"across {len({k[0] for k in REGISTRY})} schema(s)")

_schema_cache = {}
def business_schema(fire_nfris_bronze_fqn):
    """Parse schema for the Debezium payload, derived from the fire_nfris_bronze table.

    Temporal columns are read as STRING, never as their fire_nfris_bronze type: Debezium serializes SQL Server
    date/time columns as *epoch integers* (time.precision.mode=adaptive -> Timestamp=millis,
    MicroTimestamp=micros, Date=days), and from_json against a TIMESTAMP/DATE field silently yields
    NULL for a JSON number. Reading them as STRING keeps the raw value; decode_temporals() below
    converts it back. Without this, every CDC-written row loses every date column.
    """
    if fire_nfris_bronze_fqn not in _schema_cache:
        tbl = spark.table(fire_nfris_bronze_fqn)
        fields, temporal = [], []
        for f in tbl.schema.fields:
            if f.name.lower() in META_COLS:
                continue
            kind = f.dataType.typeName()            # 'timestamp' | 'timestamp_ntz' | 'date' | ...
            if kind in ("timestamp", "timestamp_ntz", "date"):
                temporal.append((f.name, kind))
                fields.append(StructField(f.name, StringType(), True))
            else:
                fields.append(f)
        _schema_cache[fire_nfris_bronze_fqn] = (StructType(fields), set(tbl.columns), temporal)
    return _schema_cache[fire_nfris_bronze_fqn]

# Columns WE stamp on every row (never sourced from the Debezium payload).
STAMPED_COLS = {"tenant_id", "_op", "source_log_position", "batch_id", "run_id", "_ingest_date", "ingestion_time"}
# Columns FILLED from the source commit time (Debezium __source_ts_ms) when the frozen capture instance
# omits them (they parse to NULL). The commit time is the moment SQL Server CDC recorded the change =
# when the row was modified (and, for an insert, created) - measured within seconds of the real value.
#   ModifiedOn -> filled on EVERY change (insert AND update): every change modifies the row, so it must
#                 advance to this change's commit time (else UPDATE keeps the stale full-load value).
#   CreatedOn  -> filled on INSERT only (a CDC insert IS a create, so created == this commit time). It is
#                 in NEVER_UPDATE_COLS below, so an UPDATE never touches it - the create time is preserved.
FILLED_COLS = {"modifiedon", "createdon"}
# CDC must NEVER overwrite these on UPDATE: set once at creation, they do not change, so an UPDATE keeps
# the existing value (full-load, or the CreatedOn stamped at CDC insert). CreatedBy/GlobalIdentifier have
# no value in the stream where the capture omits them - only the source capture-instance rebuild fills those.
NEVER_UPDATE_COLS = {"createdon", "createdby", "globalidentifier"}

def payload_columns(raw):
    """The column names the Debezium payload actually carries, for THIS table.

    The SQL Server capture instances are frozen at the schema they were created with, so they omit
    every column added later — on Elite_Mississippi that is ModifiedOn / CreatedOn / CreatedBy /
    GlobalIdentifier / FormID on ~all tables (2,389 columns across the 634 active ones). Debezium
    can only send what the change table holds, so those keys are ABSENT from the JSON (note: a
    *captured* null is still serialized as `"col":null` — absence therefore means "not captured").

    from_json types the parse schema off the fire_nfris_bronze table, so an absent key parses as NULL. Feeding
    that into the MERGE colmap would SET the column to NULL and destroy the value the full load
    loaded. We therefore merge ONLY the columns the payload actually carries: absent columns are
    left untouched on UPDATE (fire_nfris_bronze keeps the full-load value) rather than being nulled out.
    """
    keys = (raw.select(F.explode(F.map_keys(F.from_json("value_json", "map<string,string>"))).alias("k"))
               .distinct().collect())
    return {r["k"].lower() for r in keys}

def decode_temporals(df, temporal):
    """Debezium epoch-int (or ISO string) -> real timestamp/date. Unit inferred from magnitude, so it
    is correct for datetime (millis), datetime2 (micros/nanos) and datetimeoffset (ISO) alike."""
    for name, kind in temporal:
        v = F.col(f"`{name}`")
        is_int = v.rlike("^-?[0-9]+$")
        n = v.cast("decimal(38,0)")
        if kind == "date":                                   # io.debezium.time.Date = days since epoch
            conv = F.when(is_int, F.date_add(F.lit("1970-01-01").cast("date"), n.cast("int"))) \
                    .otherwise(F.to_date(v))
        else:
            a = F.abs(n)
            conv = (F.when(is_int & (a < F.lit(100000000000)),          F.timestamp_seconds(n.cast("long")))
                     .when(is_int & (a < F.lit(100000000000000)),       F.timestamp_millis(n.cast("long")))
                     .when(is_int & (a < F.lit(100000000000000000)),    F.timestamp_micros(n.cast("long")))
                     .when(is_int,                                      F.timestamp_micros((n / F.lit(1000)).cast("long")))
                     .otherwise(F.to_timestamp(v)))          # ZonedTimestamp / already-ISO
        df = df.withColumn(name, conv.cast(kind))
    return df

def ensure_deadletter():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {DLQ} (
        dead_lettered_at TIMESTAMP, run_id STRING, batch_id BIGINT, source_schema STRING,
        source_table STRING, source_db STRING, op STRING, commit_lsn STRING, error STRING, raw_value STRING
    ) USING DELTA""")
ensure_deadletter()

def dead_letter(raw_rows, tbl, err):
    """Park a failed table's RAW staged rows in the DLQ (value_json == the raw Debezium value). Replayable."""
    (raw_rows.select(
        F.current_timestamp().alias("dead_lettered_at"), F.lit(RUN_ID).alias("run_id"),
        F.col("batch_id").cast("bigint").alias("batch_id"), F.col("source_schema").alias("source_schema"),
        F.lit(tbl).alias("source_table"), F.col("source_db").alias("source_db"), F.col("op").alias("op"),
        F.col("commit_lsn").alias("commit_lsn"), F.lit(err).alias("error"), F.col("value_json").alias("raw_value"))
     .write.format("delta").mode("append").saveAsTable(DLQ))

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CURSOR} (
    tenant_id INT, source_schema STRING, source_table STRING,
    last_stage_seq BIGINT, last_batch_id BIGINT, updated_on TIMESTAMP
) USING DELTA CLUSTER BY (tenant_id, source_schema, source_table)""")

# COMMAND ----------

# DBTITLE 1,Compact ONE table: single-pass latest-op-wins MERGE into bronze.<table>. No stage/cursor writes here.
def compact_one_table(row):
    t_start = datetime.now(timezone.utc).replace(tzinfo=None)
    ss = row["source_schema"]; st = row["source_table"]
    res = {"schema_name": ss, "source_table": st, "status": None, "inserted_rows": 0,
           "batch_id": None, "error_message": None, "start_ts": t_start, "end_ts": None, "duration_secs": None}
    def _stamp():   # real per-table wall-clock (incl. contention on the shared MERGE pool) -> audit detail
        te = datetime.now(timezone.utc).replace(tzinfo=None)
        res["end_ts"] = te; res["duration_secs"] = int((te - t_start).total_seconds())
    _ss = (ss or "").strip().lower()
    if SRC_SCHEMAS is not None and _ss not in SRC_SCHEMAS:
        res["status"] = "SKIPPED"; res["error_message"] = f"schema {ss} not in run scope"
        _stamp(); return res
    # (schema, table) key — a name-only lookup resolves same-named tables from other schemas to the
    # wrong fire_nfris_bronze table (incident/patient/fieldvalue and 7 more exist in 2-3 schemas).
    meta = REGISTRY.get((_ss, (st or "").lower()))
    if not meta:
        # unregistered/inactive: leave in stage (aged out by TTL sweep). Not our table -> not dead-lettered, not deleted.
        res["status"] = "SKIPPED"; res["error_message"] = "unregistered/inactive"; _stamp(); return res
    fire_nfris_bronze_fqn = f"{TCAT}.{TSCH}.{meta['bronze_table']}"
    raw = spark.table(STAGE).filter(
        (F.col("tenant_id") == TENANT) & (F.lower("source_table") == (st or "").lower())
        & (F.lower(F.trim("source_schema")) == (ss or "").strip().lower())
        & (F.col("stage_seq") <= BOUNDARY)).cache()
    try:
        grp_batch = RUN_BATCH_UNIFIED if UNIFY else raw.select(F.max("batch_id").alias("m")).first()["m"]  # unify=1 batch/run; else per-group max
        res["batch_id"] = grp_batch
        schema, target_cols, temporal = business_schema(fire_nfris_bronze_fqn)
        # carry source_commit_ts (Debezium __source_ts_ms, staged by stage_ingest) to fill ModifiedOn below.
        # Guarded: a stage created before the column existed simply skips the fill (ModifiedOn stays as-is).
        _has_commit_ts = "source_commit_ts" in raw.columns
        # Only force-write the audit timestamps we actually filled. With a commit ts they are always
        # non-null (Debezium always sends source.ts_ms); without one (a pre-migration stage) we don't
        # touch them at all - so an UPDATE can never write a null over an existing value.
        _fill_cols = FILLED_COLS if _has_commit_ts else set()
        _base_sel = ["d.*", "op", "commit_lsn", "stage_seq"] + (["source_commit_ts"] if _has_commit_ts else [])
        parsed = raw.withColumn("d", F.from_json("value_json", schema)).select(*_base_sel)
        parsed = (decode_temporals(parsed, temporal)      # epoch-int -> timestamp/date; else every date col lands NULL
                  .withColumn("tenant_id", F.lit(TENANT).cast("int"))
                  .withColumn("_op", F.coalesce(F.col("op"), F.lit("u")))
                  .withColumn("source_log_position", F.col("commit_lsn"))
                  .withColumn("batch_id", F.lit(int(grp_batch)).cast("bigint"))
                  .withColumn("run_id", F.lit(RUN_ID))
                  .withColumn("_ingest_date", F.current_date())
                  .withColumn("ingestion_time", F.current_timestamp()))
        # Fill the audit timestamps from the source commit time when the capture omitted them (they parse
        # to NULL). coalesce keeps a real captured value if a table does carry one. ModifiedOn then flows
        # into INSERT + UPDATE; CreatedOn only into INSERT (it is in NEVER_UPDATE_COLS).
        if _has_commit_ts:
            for _fc in FILLED_COLS:
                _col = next((c for c in parsed.columns if c.lower() == _fc), None)
                if _col:
                    parsed = parsed.withColumn(_col, F.coalesce(F.col(f"`{_col}`"), F.col("source_commit_ts")))
        keys = ["tenant_id"] + meta["pk"]
        missing = [k for k in keys if k not in parsed.columns]
        if missing:
            raise Exception(f"missing key col(s) {missing}")
        # A PK that the capture instance does not carry cannot be merged on -> park it, never guess.
        present = payload_columns(raw)
        pk_uncaptured = [k for k in meta["pk"] if k.lower() not in present]
        if pk_uncaptured:
            raise Exception(f"PK col(s) not captured by CDC: {pk_uncaptured}")
        # INV-3: dedup to ONE survivor per key (highest commit_lsn, stage_seq tiebreak) BEFORE the MERGE.
        w = Window.partitionBy(*[F.col(f"`{k}`") for k in keys]).orderBy(
            F.col("commit_lsn").desc_nulls_last(), F.col("stage_seq").desc())
        latest = parsed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        # Restrict to target cols (drift-safe explicit colmap), and to columns the payload ACTUALLY
        # carries: an uncaptured column parses as NULL, and merging it would null out the full-load
        # value. Excluded from the colmap => MERGE leaves it untouched. KEEP _op for the branch cond.
        write_cols = [c for c in latest.columns
                      if c in target_cols and (c.lower() in present or c.lower() in STAMPED_COLS
                                               or c.lower() in _fill_cols)]
        res["skipped_uncaptured_cols"] = len([c for c in latest.columns
                                              if c in target_cols and c.lower() not in present
                                              and c.lower() not in STAMPED_COLS and c.lower() not in _fill_cols])
        if "_op" not in write_cols:
            write_cols = write_cols + ["_op"]
        write_df = latest.select(*[F.col(c) for c in dict.fromkeys(write_cols)])
        # Canonical-case keys: lowercase STRING key VALUES on write. The full-load lands GUIDs lowercase
        # (ADF parquet) while CDC/JDBC land them UPPERCASE; storing both cases lets the raw-merge paths
        # insert a second physical row for the same logical key (case-dupes). Normalizing every path to
        # lowercase makes the stored key canonical -> no case-dupes and fire_nfris_bronze matches the full-load/blue
        # representation. No-op for non-string keys (e.g. tenant_id/identity ints) or already-lowercase.
        _keytypes = {f.name.lower(): f.dataType.typeName() for f in write_df.schema.fields}
        for _k in keys:
            if _keytypes.get(_k.lower()) == "string":
                write_df = write_df.withColumn(_k, F.lower(F.col(_k)))
        rows_merged = write_df.count()
        colmap = {c: f"s.`{c}`" for c in write_df.columns if c in target_cols}
        # UPDATE: never touch create-once columns (keep the value); ModifiedOn overwrites with this
        # change's commit time (always non-null - it is only in the map when we had a commit ts).
        # INSERT (colmap) is unchanged - new rows get ModifiedOn and CreatedOn from the commit time.
        update_map = {c: v for c, v in colmap.items() if c.lower() not in NEVER_UPDATE_COLS}
        # Case-fold STRING keys, exactly like the full load's merge_key_condition(): the source collation
        # is SQL_Latin1_General_CP1_CI_AS (case-INsensitive), and the two paths into fire_nfris_bronze disagree on
        # GUID case (JDBC UPPERCASE vs landing lowercase). A raw compare would fail to match the same
        # logical row and INSERT a duplicate. Non-string keys (identity ints) keep plain equality.
        _ktypes = {f.name.lower(): f.dataType.typeName() for f in write_df.schema.fields}
        cond = " AND ".join(
            (f"upper(t.`{k}`) <=> upper(s.`{k}`)" if _ktypes.get(k.lower()) == "string"
             else f"t.`{k}` <=> s.`{k}`") for k in keys)
        # INV-3: ONE MERGE, branch on the survivor's op. Explicit colmap => schema-drift-safe (no updateAll/insertAll).
        (DeltaTable.forName(spark, fire_nfris_bronze_fqn).alias("t").merge(write_df.alias("s"), cond)
            .whenMatchedDelete(condition="s.`_op` = 'd'")
            .whenMatchedUpdate(set=update_map, condition="s.`_op` <> 'd'")
            .whenNotMatchedInsert(values=colmap, condition="s.`_op` <> 'd'")
            .execute())
        res["status"] = "SUCCESS"; res["inserted_rows"] = int(rows_merged)
        return res
    except Exception as e:
        # per-table isolation (INV-7): DLQ this table's RAW staged rows. The batched cleanup below then removes them.
        try:
            dead_letter(raw, st, str(e)[:300])
            res["status"] = "FAILED"; res["error_message"] = str(e)[:200]; return res
        except Exception as de:
            res["status"] = "FATAL"; res["error_message"] = f"DLQ failed: {str(de)[:120]} | orig: {str(e)[:120]}"
            return res
    finally:
        raw.unpersist()
        _stamp()

# COMMAND ----------

# DBTITLE 1,One cycle: pin boundary → per-table MERGE (parallel) → batched stage-delete + cursor + watermark + audit
# grace: only claim rows that have SETTLED (staged >= GRACE minutes ago). GRACE=0 => claim everything (blue default).
BOUNDARY = spark.sql(
    f"SELECT max(stage_seq) b FROM {STAGE} WHERE ingest_ts <= current_timestamp() - INTERVAL {GRACE} MINUTES").first()["b"]
if BOUNDARY is None:
    print(f"stage empty (or all rows within {GRACE}-min grace) — nothing to compact")
    dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "status": "EMPTY"}))
# unify: one batch_id for the whole settled slice so related rows across tables share a batch (else per-table max)
RUN_BATCH_UNIFIED = spark.sql(
    f"SELECT max(batch_id) m FROM {STAGE} WHERE tenant_id={TENANT} AND stage_seq <= {BOUNDARY}").first()["m"]

groups = (spark.table(STAGE).filter((F.col("tenant_id") == TENANT) & (F.col("stage_seq") <= BOUNDARY))
          .select("source_schema", "source_table").distinct().collect())
print(f"boundary stage_seq={BOUNDARY}  groups={len(groups)}")

# per-table MERGEs run concurrently — distinct Delta targets, so no write conflict
results = []
with ThreadPoolExecutor(max_workers=MERGE_WORKERS) as ex:
    for fut in as_completed([ex.submit(compact_one_table, r) for r in groups]):
        results.append(fut.result())

# ---- CDC lag observability: TRUE source->fire_nfris_bronze lag from Debezium __source_ts_ms, captured BEFORE the stage
# delete (rows still present). This is the reliable lag clock (source commit time, not ModifiedOn fill). ----
try:
    lag = spark.sql(f"""
      SELECT source_schema, source_table, count(*) rows_processed,
             max(source_commit_ts) newest_source_commit, min(source_commit_ts) oldest_source_commit,
             percentile_approx(unix_timestamp(current_timestamp())-unix_timestamp(source_commit_ts),0.5) median_lag_s,
             max(unix_timestamp(current_timestamp())-unix_timestamp(source_commit_ts)) max_lag_s
      FROM {STAGE}
      WHERE tenant_id={TENANT} AND stage_seq <= {BOUNDARY} AND source_commit_ts IS NOT NULL
      GROUP BY source_schema, source_table""")
    (lag.withColumn("run_id", F.lit(RUN_ID)).withColumn("run_ts", F.current_timestamp())
        .withColumn("tenant_id", F.lit(TENANT))
        .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{TCAT}.fire_nfris_control.cdc_lag_audit"))
except Exception as e:
    print("cdc_lag_audit skipped:", str(e)[:150])

# INV-2 + INV-5 (ack): ONE batched DELETE of the claimed rows for every PROCESSED table (SUCCESS: merged; FAILED:
# dead-lettered). SKIPPED/FATAL tables are left in stage. Single transaction => no concurrent-delete conflict. The
# predicate is a strict subset of the read (stage_seq <= BOUNDARY), so rows appended during this run survive.
# Keyed on schema+table: a name-only predicate would delete another schema's same-named staged rows
# (incident, patient, ... exist in 2-3 schemas each) -> silent loss of data never merged anywhere.
processed = [(r["schema_name"], r["source_table"]) for r in results if r["status"] in ("SUCCESS", "FAILED")]
if processed:
    inlist = ",".join("'" + f"{(s or '').strip().lower()}.{t.lower()}".replace("'", "''") + "'"
                      for s, t in processed)
    spark.sql(f"DELETE FROM {STAGE} WHERE tenant_id={TENANT} AND stage_seq <= {BOUNDARY} "
              f"AND concat(lower(trim(source_schema)), '.', lower(source_table)) IN ({inlist})")

run_end_ts = datetime.now(timezone.utc).replace(tzinfo=None)
# INV-6: watermark advances per SUCCESS table to the MAX staged batch_id it consumed (only after its MERGE committed)
success_updates = [(TENANT, r["schema_name"], r["source_table"], int(r["batch_id"]))
                   for r in results if r["status"] == "SUCCESS" and r.get("batch_id") is not None]
write_fire_nfris_bronze_watermarks(success_updates=success_updates)
# batched cursor advance (observability; one MERGE, no per-thread conflict)
if success_updates:
    cur = (spark.createDataFrame([(TENANT, r["schema_name"], r["source_table"], int(BOUNDARY), int(r["batch_id"]))
              for r in results if r["status"] == "SUCCESS" and r.get("batch_id") is not None],
              "tenant_id int, source_schema string, source_table string, last_stage_seq bigint, last_batch_id bigint")
           .withColumn("updated_on", F.current_timestamp()))
    try:
        (DeltaTable.forName(spark, CURSOR).alias("t").merge(cur.alias("s"),
            "t.tenant_id=s.tenant_id AND t.source_schema=s.source_schema AND t.source_table=s.source_table")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    except Exception as e:
        print("cursor skipped:", str(e)[:150])

run_batch_for_audit = max((int(r["batch_id"]) for r in results if r.get("batch_id")),
                          default=int(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
exec_id = write_fire_nfris_bronze_run_audit(tenant_id=TENANT, run_batch=run_batch_for_audit, run_start_ts=run_start_ts,
              run_end_ts=run_end_ts, pipeline_version="fire_nfris_bronze-cdc-compact-1.0",
              detail_rows=results, success_updates=success_updates)

n_ok = sum(1 for r in results if r["status"] == "SUCCESS")
n_fail = sum(1 for r in results if r["status"] in ("FAILED", "FATAL"))
n_skip = sum(1 for r in results if r["status"] == "SKIPPED")
print(f"compaction done: {n_ok} ok / {n_fail} failed / {n_skip} skipped @ boundary {BOUNDARY} (exec {exec_id})")
if [r for r in results if r["status"] == "FATAL"]:
    raise Exception(f"compaction FATAL (could not dead-letter): {[r['source_table'] for r in results if r['status']=='FATAL'][:5]}")
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "status": "ok", "boundary": int(BOUNDARY),
                                  "ok": n_ok, "failed": n_fail, "skipped": n_skip, "execution_id": exec_id}))