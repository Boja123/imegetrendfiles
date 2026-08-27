# Databricks notebook source
# MAGIC %md
# MAGIC # stage_ingest — CDC ingest = ONE append per micro-batch (no MERGE, no fan-out)
# MAGIC
# MAGIC The append half of the stage-append + periodic-MERGE re-architecture (see `cdc/KEEPUP-ARCHITECTURE.md`).
# MAGIC `readStream` Event Hub → `foreachBatch` → **one Delta append** into `medallion.fire_nfris_bronze._cdc_stage`. Per-micro-batch
# MAGIC cost is **O(rows), not O(distinct tables)** — so a schema-wide burst (the 2026-06-30 failure: ~460 tables in one
# MAGIC batch) can never stall the checkpoint. All parsing / routing / MERGE / dedup / control-table writes happen later in
# MAGIC `cdc/stage_compact.py`, off this path. This notebook is the **sole owner of the Event Hub checkpoint + consumer group**
# MAGIC (never run it alongside the old `eventhub_to_bronze_stream.py` — same checkpoint/consumer group).

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone
import json, uuid

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("eh_namespace", "eus1delteh01")
dbutils.widgets.text("eh_topic", "debezium-cdc")
dbutils.widgets.text("consumer_group", "databricks-cg")
dbutils.widgets.text("eh_secret_key", "eventhub-connection-string")
dbutils.widgets.text("secret_scope", "kv-imgtrend-dev-eus")
dbutils.widgets.text("source_schema", "FireEvent")
dbutils.widgets.text("tenant_id", "1")
dbutils.widgets.text("expected_source_db", "Elite_Develop")   # cross-check __source_db; mismatch => drop
dbutils.widgets.text("checkpoint_location", "abfss://bronze@eus1deltadls01.dfs.core.windows.net/_checkpoints/cdc_stage_ingest")
dbutils.widgets.text("starting_offsets", "latest")
dbutils.widgets.text("reset_checkpoint", "false")
dbutils.widgets.text("batch_id", "")                          # shared yyyymmddhhmmss; orchestrator passes one; empty self-mints
dbutils.widgets.text("stage_table", "medallion.fire_nfris_bronze._cdc_stage")
dbutils.widgets.text("trigger_mode", "availableNow")          # 'availableNow' (drain+stop, cron-friendly) OR 'processingTime' (continuous)
dbutils.widgets.text("processing_interval", "30s")            # used when trigger_mode=processingTime
dbutils.widgets.text("max_offsets_per_trigger", "")           # empty = no cap; append can't lag on width so the cap is no longer needed

g = lambda k: dbutils.widgets.get(k).strip()
EH_NS=g("eh_namespace"); TOPIC=g("eh_topic"); CG=g("consumer_group"); EH_KEY=g("eh_secret_key"); SCOPE=g("secret_scope")
SRC_SCHEMA=g("source_schema"); TENANT=int(g("tenant_id")); EXPECT_DB=g("expected_source_db")
CKPT=g("checkpoint_location"); START=g("starting_offsets"); STAGE=g("stage_table")
TRIGGER_MODE=g("trigger_mode"); PROC_INT=g("processing_interval"); MAXOFF=g("max_offsets_per_trigger")
RESET=g("reset_checkpoint").lower() in ("true","1","yes")
RUN_ID=str(uuid.uuid4())
_bid=g("batch_id")
if not _bid:
    try: _bid=(dbutils.jobs.taskValues.get(taskKey="init", key="batch_id", default="") or "").strip()
    except Exception: _bid=""
RUN_BATCH=int(_bid or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
print(f"ingest hub={TOPIC} tenant={TENANT} stage={STAGE} trigger={TRIGGER_MODE} run_batch={RUN_BATCH} run_id={RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Staging table — Delta, liquid-clustered on the compaction group-by keys, deletion vectors for cheap deletes
spark.sql(f"""CREATE TABLE IF NOT EXISTS {STAGE} (
    tenant_id       INT       NOT NULL,
    source_schema   STRING    NOT NULL,
    source_table    STRING    NOT NULL,
    op              STRING    NOT NULL,
    commit_lsn      STRING,
    source_db       STRING,
    source_commit_ts TIMESTAMP,   -- Debezium __source_ts_ms: when the change committed at SOURCE (true lag clock)
    eh_enqueued_ts   TIMESTAMP,   -- Event Hub record timestamp: when EH received it
    value_json      STRING    NOT NULL,
    batch_id        BIGINT    NOT NULL,
    micro_batch_id  BIGINT    NOT NULL,
    stage_seq       BIGINT    GENERATED ALWAYS AS IDENTITY,
    ingest_ts       TIMESTAMP NOT NULL,
    run_id          STRING,
    ingest_date     DATE      GENERATED ALWAYS AS (CAST(ingest_ts AS DATE))
) USING DELTA
CLUSTER BY (tenant_id, source_schema, source_table)
TBLPROPERTIES (
    'delta.enableDeletionVectors'        = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 1 days',
    'delta.logRetentionDuration'         = 'interval 3 days',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true'
)""")

# additive columns for a stage created before source_commit_ts/eh_enqueued_ts existed (idempotent)
for _c, _t in (("source_commit_ts", "TIMESTAMP"), ("eh_enqueued_ts", "TIMESTAMP")):
    try:
        spark.sql(f"ALTER TABLE {STAGE} ADD COLUMNS ({_c} {_t})")
    except Exception:
        pass   # already present

# COMMAND ----------

# DBTITLE 1,Event Hub (Kafka) source
conn = dbutils.secrets.get(SCOPE, EH_KEY)
sasl = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn}";')
kafka_opts = {
    "kafka.bootstrap.servers": f"{EH_NS}.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": sasl, "subscribe": TOPIC, "kafka.group.id": CG,
    "startingOffsets": START, "failOnDataLoss": "false",
}
if MAXOFF:
    kafka_opts["maxOffsetsPerTrigger"] = MAXOFF

# COMMAND ----------

# DBTITLE 1,foreachBatch: extract routing metadata → ONE append. No parse/registry/MERGE.
def append_batch(batch_df, micro_batch_id):
    staged = (batch_df.select(F.col("value").cast("string").alias("value_json"),
                              F.col("timestamp").alias("eh_enqueued_ts"))   # Kafka/EH record time, no JSON parse
        .withColumn("op",            F.coalesce(F.get_json_object("value_json", "$.__op"), F.lit("u")))
        # __source_ts_ms is epoch MILLIS of the source commit — the reliable end-to-end lag clock
        .withColumn("source_commit_ts", F.timestamp_millis(
                                        F.get_json_object("value_json", "$.__source_ts_ms").cast("long")))
        .withColumn("source_table",  F.coalesce(F.get_json_object("value_json", "$.__source_table"),
                                                F.get_json_object("value_json", "$.__table")))
        .withColumn("source_schema", F.coalesce(F.get_json_object("value_json", "$.__source_schema"), F.lit(SRC_SCHEMA)))
        .withColumn("source_db",     F.get_json_object("value_json", "$.__source_db"))
        .withColumn("commit_lsn",    F.coalesce(F.get_json_object("value_json", "$.__source_commit_lsn"),
                                                F.get_json_object("value_json", "$.__commit_lsn")))
        .withColumn("tenant_id",     F.lit(TENANT).cast("int"))
        .withColumn("batch_id",      F.lit(RUN_BATCH).cast("bigint"))
        .withColumn("micro_batch_id", F.lit(micro_batch_id).cast("bigint"))
        .withColumn("ingest_ts",     F.current_timestamp())
        .withColumn("run_id",        F.lit(RUN_ID))
        # same guards the old stream ran before routing, applied BEFORE the append:
        .filter(F.col("source_table").isNotNull())                                               # heartbeats/metadata-less -> skip
        .filter((F.col("source_db").isNull()) | (F.lower("source_db") == F.lit(EXPECT_DB.lower())))  # tenant cross-check
        .select("tenant_id", "source_schema", "source_table", "op", "commit_lsn", "source_db",
                "source_commit_ts", "eh_enqueued_ts",
                "value_json", "batch_id", "micro_batch_id", "ingest_ts", "run_id"))               # generated cols (stage_seq, ingest_date) auto-filled
    staged.write.format("delta").mode("append").saveAsTable(STAGE)   # THE ONLY WRITE. Checkpoint advances on this.

# COMMAND ----------

# DBTITLE 1,Run: readStream → append (checkpoint owner). trigger availableNow (cron) or processingTime (continuous).
if RESET:
    print(f"reset_checkpoint=true -> wiping {CKPT}")
    try: dbutils.fs.rm(CKPT, True)
    except Exception as e: print("checkpoint wipe note:", str(e)[:120])

writer = (spark.readStream.format("kafka").options(**kafka_opts).load()
          .writeStream.foreachBatch(append_batch).option("checkpointLocation", CKPT))
q = (writer.trigger(availableNow=True).start() if TRIGGER_MODE == "availableNow"
     else writer.trigger(processingTime=PROC_INT).start())
q.awaitTermination()
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "status": "ok", "run_batch": RUN_BATCH}))