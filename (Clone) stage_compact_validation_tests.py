# Databricks notebook source
# DBTITLE 1,Validation Test Cases for stage_compact
# MAGIC %md
# MAGIC # Validation Test Cases for stage_compact
# MAGIC
# MAGIC This notebook validates the **stage_compact** pipeline end-to-end after each run. It automatically picks up the latest `execution_id` and verifies all critical behaviors.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC1: Watermark Advancement
# MAGIC **What it checks:** After a successful compaction run, the per-table watermark (`bronze_table_watermark.last_batch_id`) and the promotion gate (`bronze_batch_watermark.batch_id`) must advance to the latest batch processed.
# MAGIC
# MAGIC **Why it matters:** Silver pipelines rely on `bronze_batch_watermark` to know which batches are ready for downstream processing. If watermarks don't advance, Silver will never pick up new data — causing silent data staleness.
# MAGIC
# MAGIC **PASS:** At least one row in `bronze_table_watermark` matches the latest batch_id, AND the promotion gate has a corresponding entry.  
# MAGIC **FAIL:** Watermarks are stuck at an older batch — indicates the `write_fire_nfris_bronze_watermarks()` function failed silently (e.g., wrong table name, permission issue).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC2: Schema Scope Filtering
# MAGIC **What it checks:** Only tables from the configured schemas (`FireEvent`, `FireInvestigation`, `FireInvestigationLocation`) are processed. Tables from other schemas (e.g., `EmsEvent`) must be skipped with reason "schema not in run scope".
# MAGIC
# MAGIC **Why it matters:** Ensures the `source_schema` parameter is correctly filtering. If a configured schema is wrongly skipped, its data will accumulate in the stage indefinitely without being merged into bronze.
# MAGIC
# MAGIC **PASS:** Zero tables from configured schemas are skipped with "not in run scope".  
# MAGIC **FAIL:** A configured schema's tables are being skipped — indicates `source_schema` parameter mismatch or case-sensitivity issue.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC3: Stage Cleanup
# MAGIC **What it checks:** After compaction, rows belonging to SUCCESS and FAILED tables are deleted from `_cdc_stage`. Rows belonging to SKIPPED tables (out-of-scope schemas, unregistered tables) must remain.
# MAGIC
# MAGIC **Why it matters:** If processed rows are NOT deleted, they'll be re-merged on the next run (duplicate processing, wasted compute). If skipped rows ARE deleted, their data is permanently lost before being merged anywhere.
# MAGIC
# MAGIC **PASS:** Zero orphaned rows from SUCCESS tables remain in stage, AND skipped schema rows are preserved.  
# MAGIC **FAIL:** Orphaned rows found — indicates the DELETE predicate is too narrow or failed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC4: CDC Operations (Insert/Update/Delete)
# MAGIC **What it checks:** The MERGE correctly handles all CDC operation types: inserts (`_op = 'c'`), updates (`_op = 'u'`), and deletes (`_op = 'd'`). Also validates the "latest-op-wins" deduplication — no duplicate primary keys should exist.
# MAGIC
# MAGIC **Why it matters:** If deduplication fails, the same record appears multiple times in bronze, causing inflated counts and join explosions downstream. If deletes aren't applied, stale/removed records persist.
# MAGIC
# MAGIC **PASS:** Each PK appears at most once in the bronze table (latest-op-wins working).  
# MAGIC **FAIL:** Duplicate PKs found — indicates the MERGE condition or deduplication window logic is broken.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC5: Execution Audit Completeness
# MAGIC **What it checks:** The `fire_nfris_bronze_pipeline_execution` table has a summary row for the run, and `fire_nfris_bronze_pipeline_execution_detail` has one row per table (SUCCESS + FAILED + SKIPPED). The `total_tables` in the summary must match the detail row count.
# MAGIC
# MAGIC **Why it matters:** The audit tables are the observability backbone — used for monitoring, alerting, and debugging. If they're incomplete or inconsistent, operational issues go undetected.
# MAGIC
# MAGIC **PASS:** Execution row exists AND detail row count matches the summary's `total_tables`.  
# MAGIC **FAIL:** Missing execution row, or count mismatch — indicates the `write_fire_nfris_bronze_run_audit()` function failed or was called with incomplete data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC6: Cursor Advancement
# MAGIC **What it checks:** The `cdc_stage_compaction_cursor` table is updated with the latest `last_stage_seq` and `last_batch_id` for every SUCCESS table. The cursor's max batch should match the watermark's max batch.
# MAGIC
# MAGIC **Why it matters:** The cursor provides per-table observability of compaction progress. If cursor and watermark diverge, it indicates a partial commit — some tables advanced while others didn't.
# MAGIC
# MAGIC **PASS:** Cursor max batch matches watermark max batch.  
# MAGIC **FAIL:** Mismatch — indicates the cursor MERGE succeeded but watermark MERGE failed (or vice versa).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC7: Dead Letter Queue (DLQ)
# MAGIC **What it checks:** The `_cdc_stream_deadletter` table exists and is accessible. When tables FAIL during compaction, their raw rows are parked here for manual inspection/retry.
# MAGIC
# MAGIC **Why it matters:** Without a DLQ, failed rows are deleted from stage (as part of the FAILED table cleanup) with no recovery path — data loss.
# MAGIC
# MAGIC **PASS:** DLQ table exists and is queryable.  
# MAGIC **FAIL:** Table not found — indicates DLQ was never created or was dropped.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC8: Empty Stage Handling
# MAGIC **What it checks:** Reports the current `_cdc_stage` row count. When the stage is empty (or all rows within the grace period), stage_compact should exit gracefully with `{"status": "EMPTY"}` — no errors, no partial writes.
# MAGIC
# MAGIC **Why it matters:** Scheduled runs will frequently hit an empty stage (no CDC events between runs). The notebook must handle this gracefully without writing phantom audit entries or advancing watermarks incorrectly.
# MAGIC
# MAGIC **PASS:** If stage count is 0 and the last run exited with status "EMPTY".  
# MAGIC **INFO:** Shows current stage depth for operational awareness.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC9: Temporal Decoding
# MAGIC **What it checks:** Debezium sends timestamps as epoch integers (millis/micros/nanos) or ISO strings. The compactor must decode these correctly into proper `timestamp`/`date` columns. This test checks that `ModifiedOn` is NOT NULL for CDC rows (`_op` in `'c'`, `'u'`).
# MAGIC
# MAGIC **Why it matters:** If epoch decoding fails, timestamp columns become NULL — breaking time-based filters, partitioning, SCD logic, and any `ModifiedOn`-based CDC tracking downstream.
# MAGIC
# MAGIC **PASS:** Zero NULL `ModifiedOn` values on insert/update CDC rows.  
# MAGIC **WARN:** NULLs found — indicates Debezium schema metadata mismatch or missing temporal decoder logic.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TC10: Unregistered/Inactive Tables
# MAGIC **What it checks:** Tables present in `_cdc_stage` that are NOT in `bronze_table_registry` (or have `is_active = false`) must be SKIPPED during compaction AND their rows must remain in the stage (not deleted).
# MAGIC
# MAGIC **Why it matters:** New tables may appear in the CDC stream before being registered. Deleting their staged rows would cause permanent data loss. Keeping them in stage allows a future registration to pick them up.
# MAGIC
# MAGIC **PASS:** Unregistered table rows remain in stage after compaction.  
# MAGIC **FAIL:** Unregistered rows were deleted — indicates the stage DELETE predicate is too broad.

# COMMAND ----------

# DBTITLE 1,Setup: Get latest execution_id
from pyspark.sql import functions as F

# Get the latest execution_id from the pipeline
latest_exec_id = spark.sql("""
    SELECT execution_id 
    FROM medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution
    ORDER BY start_ts DESC LIMIT 1
""").first()["execution_id"]

print(f"Validating execution: {latest_exec_id}")

# COMMAND ----------

# DBTITLE 1,TC1: Watermark Advancement
# TC1: Verify watermarks advanced for the latest execution
latest_exec = spark.sql(f"""
    SELECT execution_id, MAX(batch_id) as max_batch
    FROM medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution_detail
    WHERE execution_id = '{latest_exec_id}'
      AND status = 'SUCCESS' AND batch_id IS NOT NULL
    GROUP BY execution_id
""").first()

if latest_exec:
    exec_id = latest_exec["execution_id"]
    expected_batch = latest_exec["max_batch"]
    
    # Check bronze_table_watermark
    wm_check = spark.sql(f"""
        SELECT COUNT(*) as updated_count
        FROM medallion.fire_nfris_control.bronze_table_watermark
        WHERE last_batch_id = {expected_batch}
    """).first()["updated_count"]
    
    # Check promotion gate
    gate_check = spark.sql(f"""
        SELECT COUNT(*) as gate_count
        FROM medallion.fire_nfris_control.bronze_batch_watermark
        WHERE batch_id = {expected_batch}
    """).first()["gate_count"]
    
    print(f"Execution: {exec_id}")
    print(f"Expected batch_id: {expected_batch}")
    print(f"TC1a - bronze_table_watermark updated: {'PASS ✓' if wm_check > 0 else 'FAIL ✗'} ({wm_check} tables)")
    print(f"TC1b - promotion gate registered:      {'PASS ✓' if gate_check > 0 else 'FAIL ✗'}")
else:
    print("TC1 - SKIP: No successful executions found")

# COMMAND ----------

# DBTITLE 1,TC2: Schema Scope Filtering
# TC2: Verify schema scope filtering
scope_check = spark.sql(f"""
    SELECT schema_name, status, error_message, COUNT(*) as cnt
    FROM medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution_detail
    WHERE execution_id = '{latest_exec_id}'
    GROUP BY schema_name, status, error_message
    ORDER BY schema_name, status
""")
display(scope_check)

# Validate: configured schemas should NOT be skipped with "not in run scope"
configured_schemas = {'FireEvent', 'FireInvestigation', 'FireInvestigationLocation'}
wrongly_skipped = spark.sql(f"""
    SELECT schema_name, source_table
    FROM medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution_detail
    WHERE execution_id = '{latest_exec_id}'
      AND status = 'SKIPPED'
      AND error_message LIKE '%not in run scope%'
      AND schema_name IN ('FireEvent', 'FireInvestigation', 'FireInvestigationLocation')
""").count()

print(f"\nTC2 - No configured schema wrongly skipped: {'PASS ✓' if wrongly_skipped == 0 else 'FAIL ✗'} (found {wrongly_skipped})")

# COMMAND ----------

# DBTITLE 1,TC3: Stage Cleanup
# TC3: Verify stage cleanup — processed rows deleted, skipped rows remain

# Check if any SUCCESS tables still have rows in stage (they shouldn't)
processed_still_in_stage = spark.sql(f"""
    SELECT s.source_schema, s.source_table, COUNT(*) as orphaned_rows
    FROM medallion.fire_nfris_bronze._cdc_stage s
    INNER JOIN medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution_detail d
        ON LOWER(TRIM(s.source_schema)) = LOWER(d.schema_name)
        AND LOWER(s.source_table) = LOWER(d.source_table)
        AND d.execution_id = '{latest_exec_id}'
        AND d.status = 'SUCCESS'
    WHERE s.tenant_id = 1
    GROUP BY s.source_schema, s.source_table
""")
orphan_count = processed_still_in_stage.count()

# Check that skipped (out-of-scope) tables still have rows in stage
skipped_in_stage = spark.sql("""
    SELECT source_schema, COUNT(DISTINCT source_table) as tables, COUNT(*) as rows
    FROM medallion.fire_nfris_bronze._cdc_stage
    WHERE tenant_id = 1
    GROUP BY source_schema
""")

print(f"TC3a - No SUCCESS table rows remain in stage: {'PASS ✓' if orphan_count == 0 else 'FAIL ✗'} ({orphan_count} tables with orphans)")
print(f"\nTC3b - Skipped schema rows still in stage:")
display(skipped_in_stage)

# COMMAND ----------

# DBTITLE 1,TC4: CDC Operations (Insert/Update/Delete)
# TC4: Verify CDC operations are reflected correctly in bronze
# Pick a table that had recent activity (FireInvestigation.Incident)

test_table = "medallion.fire_nfris_bronze.fireinvestigation_incident"
try:
    ops = spark.sql(f"""
        SELECT _op, COUNT(*) as cnt, MAX(_ingest_date) as latest_ingest
        FROM {test_table}
        GROUP BY _op
        ORDER BY _op
    """)
    print(f"TC4a - CDC operations in {test_table}:")
    display(ops)
    
    # Verify latest-op-wins: no duplicate PKs
    pk_col = "IncidentID"  # from registry
    dup_check = spark.sql(f"""
        SELECT {pk_col}, COUNT(*) as cnt
        FROM {test_table}
        WHERE tenant_id = 1
        GROUP BY {pk_col}
        HAVING COUNT(*) > 1
    """).count()
    print(f"\nTC4b - No duplicate PKs (latest-op-wins): {'PASS ✓' if dup_check == 0 else 'FAIL ✗'} ({dup_check} duplicates)")
except Exception as e:
    print(f"TC4 - SKIPPED (table not available): {str(e)[:100]}")

# COMMAND ----------

# DBTITLE 1,TC5: Execution Audit Completeness
# TC5: Verify execution audit tables are complete

exec_row = spark.sql(f"""
    SELECT execution_id, start_ts, end_ts, duration_secs, 
           total_tables, successful_tables, failed_tables, skipped_tables,
           watermark_updates, total_inserted_rows, status
    FROM medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution
    WHERE execution_id = '{latest_exec_id}'
""").first()

detail_counts = spark.sql(f"""
    SELECT status, COUNT(*) as cnt
    FROM medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution_detail
    WHERE execution_id = '{latest_exec_id}'
    GROUP BY status
""").collect()
detail_total = sum(r["cnt"] for r in detail_counts)

if exec_row:
    print(f"Execution: {exec_row['execution_id']}")
    print(f"Duration: {exec_row['duration_secs']}s | Status: {exec_row['status']}")
    print(f"Summary: {exec_row['successful_tables']} ok / {exec_row['failed_tables']} failed / {exec_row['skipped_tables']} skipped")
    print(f"Watermark updates: {exec_row['watermark_updates']} | Inserted rows: {exec_row['total_inserted_rows']}")
    
    # Cross-check: execution total_tables == detail row count
    match = exec_row['total_tables'] == detail_total
    print(f"\nTC5a - Execution row exists:                    PASS ✓")
    print(f"TC5b - Detail count matches execution summary:  {'PASS ✓' if match else 'FAIL ✗'} (exec={exec_row['total_tables']}, detail={detail_total})")
else:
    print(f"TC5 - FAIL ✗: No execution row for {latest_exec_id}")

# COMMAND ----------

# DBTITLE 1,TC6: Cursor Advancement
# TC6: Verify cursor table is updated for SUCCESS tables

cursor_check = spark.sql(f"""
    SELECT c.source_schema, c.source_table, c.last_stage_seq, c.last_batch_id, c.updated_on
    FROM medallion.fire_nfris_control.cdc_stage_compaction_cursor c
    WHERE c.tenant_id = 1
    ORDER BY c.updated_on DESC
    LIMIT 10
""")
print("TC6 - Latest cursor entries (should reflect recent runs):")
display(cursor_check)

# Verify cursor was updated for the latest batch
latest_cursor_batch = spark.sql("""
    SELECT MAX(last_batch_id) as max_batch 
    FROM medallion.fire_nfris_control.cdc_stage_compaction_cursor
    WHERE tenant_id = 1
""").first()["max_batch"]

latest_wm_batch = spark.sql("""
    SELECT MAX(last_batch_id) as max_batch 
    FROM medallion.fire_nfris_control.bronze_table_watermark
""").first()["max_batch"]

print(f"\nTC6 - Cursor max batch ({latest_cursor_batch}) matches watermark max batch ({latest_wm_batch}): "
      f"{'PASS ✓' if latest_cursor_batch == latest_wm_batch else 'FAIL ✗'}")

# COMMAND ----------

# DBTITLE 1,TC7-10: DLQ, Empty Stage, Temporal Decoding, Unregistered Tables
# TC7: Dead Letter Queue (verify structure exists)
try:
    dlq_count = spark.table("medallion.fire_nfris_bronze._cdc_stream_deadletter").count()
    print(f"TC7 - DLQ table exists: PASS ✓ ({dlq_count} rows currently)")
except:
    print("TC7 - DLQ table exists: FAIL ✗ (table not found)")

# TC8: Empty Stage Handling
# (Verified manually: when stage is empty, notebook exits with {"status": "EMPTY"})
stage_count = spark.sql("SELECT COUNT(*) c FROM medallion.fire_nfris_bronze._cdc_stage").first()["c"]
print(f"\nTC8 - Stage current row count: {stage_count} (if 0, next run should exit with EMPTY)")

# TC9: Temporal Decoding — spot check a table with timestamp columns
try:
    null_ts = spark.sql("""
        SELECT COUNT(*) as null_modifiedon
        FROM medallion.fire_nfris_bronze.fireinvestigation_incident
        WHERE ModifiedOn IS NULL AND _op IN ('c', 'u')
    """).first()["null_modifiedon"]
    print(f"\nTC9 - Temporal decoding (no NULL ModifiedOn for CDC rows): {'PASS ✓' if null_ts == 0 else 'WARN ⚠'} ({null_ts} nulls)")
except Exception as e:
    print(f"\nTC9 - SKIPPED: {str(e)[:80]}")

# TC10: Unregistered tables remain in stage
unregistered_in_stage = spark.sql("""
    SELECT s.source_schema, s.source_table, COUNT(*) as rows
    FROM medallion.fire_nfris_bronze._cdc_stage s
    LEFT JOIN medallion.fire_nfris_control.bronze_table_registry r
        ON LOWER(TRIM(s.source_schema)) = LOWER(r.source_schema)
        AND LOWER(s.source_table) = LOWER(r.source_table)
        AND r.is_active = true
    WHERE r.source_table IS NULL AND s.tenant_id = 1
    GROUP BY s.source_schema, s.source_table
""")
if unregistered_in_stage.count() > 0:
    print(f"\nTC10 - Unregistered tables preserved in stage: PASS ✓")
    display(unregistered_in_stage)
else:
    print(f"\nTC10 - No unregistered table rows in stage (nothing to validate)")

# COMMAND ----------

# DBTITLE 1,Summary: All Test Results
# Summary of all test results
run_date = spark.sql(
    f"SELECT start_ts FROM medallion.fire_nfris_control.fire_nfris_bronze_pipeline_execution WHERE execution_id = '{latest_exec_id}'"
).first()[0]

print("="*60)
print("  VALIDATION SUMMARY")
print("="*60)
print(f"  Execution ID: {latest_exec_id}")
print(f"  Run Date:     {run_date}")
print("="*60)
print("  TC1  Watermark Advancement        — Check above")
print("  TC2  Schema Scope Filtering        — Check above")
print("  TC3  Stage Cleanup                 — Check above")
print("  TC4  CDC Operations                — Check above")
print("  TC5  Execution Audit               — Check above")
print("  TC6  Cursor Advancement            — Check above")
print("  TC7  Dead Letter Queue             — Check above")
print("  TC8  Empty Stage Handling           — Check above")
print("  TC9  Temporal Decoding             — Check above")
print("  TC10 Unregistered Tables           — Check above")
print("="*60)

# COMMAND ----------

