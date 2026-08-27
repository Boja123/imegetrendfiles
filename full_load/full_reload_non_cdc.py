# Databricks notebook source
# MAGIC %md
# MAGIC # full_reload_non_cdc — daily full reload of is_active tables that nothing else keeps current
# MAGIC
# MAGIC The third "road": for `is_active` tables that are **not** owned by the CDC stream, re-read the whole
# MAGIC table from the source and MERGE it into `medallion.bronze.<bronze_table>` by `[tenant_id] + pk`. This is
# MAGIC what keeps tables with **no CDC** (and no `ModifiedOn` watermark) current — e.g. the FireEvent / Resource
# MAGIC schemas, which the EmsEvent-only Debezium stream never touches.
# MAGIC
# MAGIC ### Safe by design
# MAGIC * **Skips only what the stream actually captures** — a table in a `stream_schemas` schema (EmsEvent) that
# MAGIC   also has CDC on. NOTE: CDC may be *enabled* on tables the connector doesn't capture (e.g. FireEvent), so
# MAGIC   "has CDC on" alone isn't enough — those are still full-reloaded here, otherwise they'd be covered by no road.
# MAGIC * **MERGE by `[tenant_id] + pk`** — non-destructive upsert. A failed read/MERGE loses nothing; the table
# MAGIC   just retries on the next run (idempotent). Tables with no PK are **skipped + logged** (can't MERGE safely).
# MAGIC * **Per-table isolation** — one table failing doesn't fail the others; the run reports PARTIAL and the bad
# MAGIC   table retries next time.
# MAGIC * **DEV only. Source is READ-ONLY.** Secrets only from the Key-Vault scope.
# MAGIC
# MAGIC ### Known limitation
# MAGIC MERGE-upsert reflects inserts + updates, but **not hard deletes** (a row removed at source stays in Bronze).
# MAGIC Truncates are handled out-of-band (disable CDC → truncate → re-enable → re-snapshot via the stream). If exact
# MAGIC delete-mirroring is ever needed here, add a `WHEN NOT MATCHED BY SOURCE ... THEN DELETE` clause scoped to the
# MAGIC tenant.

# COMMAND ----------

import json, time, uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Feed the team's canonical Bronze control tables (feedback #4) + the Silver keystone
def write_bronze_control_metadata(*, tenant_id, run_batch, run_start_ts, run_end_ts,
                                  pipeline_version, detail_rows, success_updates, dry=False):
    """One place that feeds the team's EXISTING control tables so the Silver pipeline works automatically.
      - control.batch_watermark            : the table the Silver loader actually reads (its keystone). KEPT.
      - control.bronze_table_watermark     : team per-table resume cursor (same shape, bronze_-prefixed).
      - control.bronze_pipeline_execution(_detail) : team run-header + per-table audit ledger (replaces the
                                             bespoke streaming_audit_log). execution_id = yyyymmddHHMMSS+micros.
      - control.bronze_batch_watermark     : run-level promotion gate (insert batch_id only if no table FAILED).
    detail_rows: [{schema_name, source_table, status in {SUCCESS,FAILED,SKIPPED}, inserted_rows, error_message}]
    success_updates: [(tenant_id, schema_name, source_table, last_batch_id)] for SUCCESS tables.
    Returns the execution_id. Mirrors imagetrend-pipelines/.../landing-bronze full load pipeline.py schemas."""
    from decimal import Decimal
    execution_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    rb = int(run_batch); dur = int((run_end_ts - run_start_ts).total_seconds())
    created = datetime.now(timezone.utc).replace(tzinfo=None)
    if dry:
        print("control-metadata: dry_run -> no control-table writes"); return execution_id

    # per-table cursors: batch_watermark (Silver keystone) + bronze_table_watermark (team). Explicit columns,
    # NOT insertAll, so the shared tables' identity 'id' column is left to auto-generate and the schema is untouched.
    if success_updates:
        wm = (spark.createDataFrame(success_updates,
                 "tenant_id int, schema_name string, source_table string, last_batch_id bigint")
              .withColumn("updated_on", F.current_timestamp()))
        for tbl in ("medallion.control.batch_watermark", "medallion.control.bronze_table_watermark"):
            try:
                (DeltaTable.forName(spark, tbl).alias("t").merge(wm.alias("s"),
                    "t.tenant_id=s.tenant_id AND t.schema_name=s.schema_name AND t.source_table=s.source_table")
                    .whenMatchedUpdate(set={"last_batch_id": "s.last_batch_id", "updated_on": "s.updated_on"})
                    .whenNotMatchedInsert(values={"tenant_id": "s.tenant_id", "schema_name": "s.schema_name",
                          "source_table": "s.source_table", "last_batch_id": "s.last_batch_id",
                          "updated_on": "s.updated_on"}).execute())
                print(f"{tbl}: upserted {len(success_updates)} cursors @ {rb}")
            except Exception as e:
                print(f"{tbl} skipped:", str(e)[:150])

    n_ok = sum(1 for r in detail_rows if r["status"] == "SUCCESS")
    n_fail = sum(1 for r in detail_rows if r["status"] == "FAILED")
    n_skip = sum(1 for r in detail_rows if r["status"] == "SKIPPED")

    # per-table audit -> bronze_pipeline_execution_detail (append)
    detail_ddl = ("execution_id string, tenant_id int, schema_name string, source_table string, "
                  "batch_id bigint, status string, inserted_rows bigint, error_message string, "
                  "start_ts timestamp, end_ts timestamp, duration_secs bigint, created_ts timestamp")
    drows = [(execution_id, int(tenant_id), r["schema_name"], r["source_table"],
              (rb if r["status"] != "SKIPPED" else None), r["status"], int(r.get("inserted_rows") or 0),
              (r.get("error_message") or None), run_start_ts, run_end_ts, dur, created) for r in detail_rows]
    try:
        if drows:
            spark.createDataFrame(drows, schema=detail_ddl).write.mode("append").saveAsTable(
                "medallion.control.bronze_pipeline_execution_detail")
            print(f"bronze_pipeline_execution_detail: +{len(drows)} (exec {execution_id})")
    except Exception as e:
        print("bronze_pipeline_execution_detail skipped:", str(e)[:150])

    # run header -> bronze_pipeline_execution (append). One batch per run, so batch counters mirror table counters.
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
            "medallion.control.bronze_pipeline_execution")
        print(f"bronze_pipeline_execution: header (exec {execution_id}, {status})")
    except Exception as e:
        print("bronze_pipeline_execution skipped:", str(e)[:150])

    # run-level promotion gate -> bronze_batch_watermark: promote batch_id only if nothing FAILED and it is new.
    try:
        if n_fail == 0 and success_updates:
            last = spark.sql("SELECT MAX(batch_id) m FROM medallion.control.bronze_batch_watermark").first()["m"]
            if rb > (last if last is not None else -1):
                bw = spark.createDataFrame([(rb, created, execution_id)],
                        "batch_id bigint, completed_ts timestamp, execution_id string")
                (DeltaTable.forName(spark, "medallion.control.bronze_batch_watermark").alias("t")
                    .merge(bw.alias("s"), "t.batch_id=s.batch_id")
                    .whenNotMatchedInsert(values={"batch_id": "s.batch_id", "completed_ts": "s.completed_ts",
                          "execution_id": "s.execution_id"}).execute())
                print(f"bronze_batch_watermark: promoted {rb}")
    except Exception as e:
        print("bronze_batch_watermark skipped:", str(e)[:150])
    return execution_id

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("source_database", "Elite_Develop")
dbutils.widgets.text("registry_table", "medallion.control.bronze_table_registry")
dbutils.widgets.text("target_catalog", "medallion")
dbutils.widgets.text("target_schema", "bronze")
dbutils.widgets.text("tenant_id", "1")
dbutils.widgets.text("secret_scope", "kv-imgtrend-dev-eus")
dbutils.widgets.text("jdbc_host", "")
dbutils.widgets.text("jdbc_port", "3342")
dbutils.widgets.text("jdbc_user", "PipelineUser")
dbutils.widgets.text("secret_key_password", "debezium-db-password")
# auth_mode: "sql" (dev default: user/password) | "aad" (green/blue: data-platform-reader SP token)
dbutils.widgets.text("auth_mode", "sql")
dbutils.widgets.text("trust_server_certificate", "true")
dbutils.widgets.text("skip_if_cdc", "true")     # skip tables the CDC stream actually captures (see stream_schemas)
dbutils.widgets.text("stream_schemas", "EmsEvent")  # schemas the Debezium connector captures (include_list). A table
#   is "owned by the stream" only if it is BOTH in one of these schemas AND has CDC on. CDC enabled on a table in
#   another schema (e.g. FireEvent) does NOT mean the stream captures it — so we still full-reload it here.
dbutils.widgets.text("schemas", "")             # optional CSV to limit schemas; empty = every is_active schema
dbutils.widgets.text("max_workers", "8")
# write_mode: "merge" (default — daily incremental reload, idempotent upsert) vs "overwrite_tenant"
#   (initial backfill of a brand-new client — tenant-scoped replaceWhere, no anti-join against other
#   tenants; bronze is Delta clustered by tenant_id so this rewrites only THIS tenant's files and is
#   idempotent on re-run). The onboarding automation passes overwrite_tenant for a from-scratch client.
dbutils.widgets.text("write_mode", "merge")
# read_partitions: >1 splits each single-numeric-PK source read across N JDBC connections (SQL Server
#   partitionColumn). Turns a single-connection sequential pull of a large table into a parallel read.
#   1 = legacy single-connection behavior (unchanged for the daily reload).
dbutils.widgets.text("read_partitions", "1")
dbutils.widgets.text("dry_run", "false")
dbutils.widgets.text("audit_table", "medallion.control.streaming_audit_log")
dbutils.widgets.text("batch_id", "")  # shared yyyymmddhhmmss; orchestrator passes one common id, empty self-mints

g = lambda k: dbutils.widgets.get(k).strip()
SRC_DB=g("source_database"); REG=g("registry_table"); TCAT=g("target_catalog"); TSCH=g("target_schema")
TENANT=int(g("tenant_id")); SCOPE=g("secret_scope")
HOST=g("jdbc_host"); PORT=g("jdbc_port") or "3342"; USER=g("jdbc_user"); PWKEY=g("secret_key_password")
AUTH=g("auth_mode").lower() or "sql"
TRUST=g("trust_server_certificate").lower() in ("true","1","yes")
SKIP_CDC=g("skip_if_cdc").lower()=="true"
STREAM_SCHEMAS={s.strip().lower() for s in g("stream_schemas").split(",") if s.strip()} or {"emsevent"}
SCHEMAS={s.strip().lower() for s in g("schemas").split(",") if s.strip()}
MAXW=int(g("max_workers") or "8"); DRY=g("dry_run").lower()=="true"; AUDIT=g("audit_table")
WMODE=(g("write_mode").lower() or "merge"); RPARTS=max(1,int(g("read_partitions") or "1"))
RUN_ID=str(uuid.uuid4())
_bid = g("batch_id")
if not _bid:  # in the orchestrator, read the ONE common id the init task minted; standalone => self-mint
    try: _bid = (dbutils.jobs.taskValues.get(taskKey="init", key="batch_id", default="") or "").strip()
    except Exception: _bid = ""
RUN_BATCH=int(_bid or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
run_start_ts=datetime.now(timezone.utc).replace(tzinfo=None)
assert HOST, "jdbc_host is required"
URL=(f"jdbc:sqlserver://{HOST}:{PORT};databaseName={SRC_DB};"
     f"encrypt=true;trustServerCertificate={'true' if TRUST else 'false'};loginTimeout=30;")

def _aad_token():
    import requests
    tid=dbutils.secrets.get(SCOPE,"data-platform-reader-tenant-id")
    cid=dbutils.secrets.get(SCOPE,"data-platform-reader-client-id")
    sec=dbutils.secrets.get(SCOPE,"data-platform-reader-client-secret")
    r=requests.post(f"https://login.microsoftonline.com/{tid}/oauth2/v2.0/token",
        data={"grant_type":"client_credentials","client_id":cid,"client_secret":sec,
              "scope":"https://database.windows.net/.default"}, timeout=30)
    r.raise_for_status(); return r.json()["access_token"]

# SQL auth (dev) fetches the password secret; AAD auth (green/blue) uses an SP access token.
PW = None if AUTH=="aad" else dbutils.secrets.get(SCOPE, PWKEY)

def _auth(r):
    return r.option("accessToken", _aad_token()) if AUTH=="aad" else r.option("user", USER).option("password", PW)

def jdbc(q):
    r=(spark.read.format("jdbc").option("url", URL)
       .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("fetchsize","10000")
       .option("query", q))
    return _auth(r).load()

def read_source(ss, st, pk):
    """Read a full source table. With read_partitions>1 and a single numeric PK, split the pull across
    N JDBC connections (partitionColumn); otherwise a single-connection read (legacy behavior)."""
    tbl=f"[{ss}].[{st}]"
    if RPARTS>1 and len(pk)==1:
        try:
            b=jdbc(f"SELECT MIN([{pk[0]}]) lo, MAX([{pk[0]}]) hi FROM {tbl}").collect()[0]
            lo,hi=b["lo"],b["hi"]
            if isinstance(lo,int) and isinstance(hi,int) and hi>lo:
                r=(spark.read.format("jdbc").option("url", URL)
                   .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("fetchsize","10000")
                   .option("dbtable", tbl).option("partitionColumn", pk[0])
                   .option("lowerBound", str(lo)).option("upperBound", str(hi))
                   .option("numPartitions", str(RPARTS)))
                return _auth(r).load()
        except Exception:
            pass  # non-numeric PK / min-max failure => fall back to a single-connection read
    return jdbc(f"SELECT * FROM {tbl}")

print(f"source={SRC_DB} tenant={TENANT} target={TCAT}.{TSCH} skip_if_cdc={SKIP_CDC} "
      f"write_mode={WMODE} read_partitions={RPARTS} dry_run={DRY} run_id={RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Build the work list: is_active, NOT CDC-owned, has a PK
rows=(spark.table(REG).filter("is_active = true")
      .select("source_schema","source_table","bronze_table","pk_columns").collect())

cdc_set=set()
if SKIP_CDC:
    cdc_set={(r["ss"].lower(), r["st"].lower()) for r in jdbc(
        "SELECT s.name ss, t.name st FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id "
        "WHERE t.is_tracked_by_cdc=1").collect()}

todo=[]; skipped_cdc=[]; skipped_nopk=[]
for r in rows:
    ss=r["source_schema"]; st=r["source_table"]; bt=r["bronze_table"]
    pk=[c.strip() for c in (r["pk_columns"] or "").split(",") if c.strip()]
    if SCHEMAS and ss.lower() not in SCHEMAS: continue
    # skip ONLY tables the stream actually captures = in a stream schema AND CDC-on. CDC-on in another schema
    # (FireEvent/Resource) is NOT captured by the EmsEvent-only connector, so we still full-reload it here.
    if SKIP_CDC and ss.lower() in STREAM_SCHEMAS and (ss.lower(), st.lower()) in cdc_set:
        skipped_cdc.append(f"{ss}.{st}"); continue
    if not bt or not pk: skipped_nopk.append(f"{ss}.{st}"); continue
    todo.append((ss, st, bt, pk))

print(f"is_active={len(rows)}  to_full_reload={len(todo)}  skipped_cdc(stream owns)={len(skipped_cdc)}  "
      f"skipped_no_pk={len(skipped_nopk)}")

# COMMAND ----------

# DBTITLE 1,Reload one table  (full read -> shape -> MERGE by [tenant_id]+pk)
def reload_one(ss, st, bt, pk):
    base={"ss":ss,"st":st,"bt":bt,"t":f"{ss}.{st}"}
    fqn=f"{TCAT}.{TSCH}.{bt}"
    try:
        src=(read_source(ss, st, pk)
             .withColumn("tenant_id", F.lit(TENANT).cast("int"))
             .withColumn("_op", F.lit("r"))
             .withColumn("batch_id", F.lit(RUN_BATCH).cast("bigint"))
             .withColumn("run_id", F.lit(RUN_ID))
             .withColumn("_ingest_date", F.current_date())
             .withColumn("ingestion_time", F.current_timestamp()))
        keys=["tenant_id"]+pk
        missing=[k for k in keys if k not in src.columns]
        if missing:
            return {**base,"error":f"missing key col(s) {missing}"}
        # defensive de-dupe on the merge key (a true PK won't collide; guards against source weirdness)
        w=Window.partitionBy(*[F.col(f"`{k}`") for k in keys]).orderBy(F.lit(1))
        src=src.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn").cache()
        try:
            n=src.count()
            if DRY:
                return {**base,"rows":n,"dry":True}
            if not spark.catalog.tableExists(fqn):
                src.limit(0).write.saveAsTable(fqn)   # create with the catalog default format (Iceberg) on first load
            target_cols=set(spark.table(fqn).columns)
            write=src.select(*[c for c in src.columns if c in target_cols])
            if WMODE=="overwrite_tenant":
                # initial backfill: replace ONLY this tenant's slice. Bronze is Delta clustered by tenant_id,
                # so replaceWhere rewrites just this tenant's files (no anti-join against other tenants) and is
                # idempotent on re-run. Never touches tenants already in the table.
                (write.write.format("delta").mode("overwrite")
                     .option("replaceWhere", f"tenant_id = {TENANT}").saveAsTable(fqn))
            else:
                cond=" AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
                # map ONLY the columns the source has (not updateAll/insertAll): target-only columns such as
                # source_log_position (CDC-only) or any schema drift are then left untouched on update / NULL on
                # insert, instead of erroring with DELTA_MERGE_UNRESOLVED_EXPRESSION.
                colmap={c: f"s.`{c}`" for c in write.columns}
                (DeltaTable.forName(spark, fqn).alias("t").merge(write.alias("s"), cond)
                    .whenMatchedUpdate(set=colmap).whenNotMatchedInsert(values=colmap).execute())
            return {**base,"rows":n,"ok":True}
        finally:
            src.unpersist()
    except Exception as e:
        return {**base,"error":str(e)[:200]}

results=[]
with ThreadPoolExecutor(max_workers=MAXW) as ex:
    for fut in as_completed([ex.submit(reload_one, *t) for t in todo]):
        results.append(fut.result())

# COMMAND ----------

# DBTITLE 1,Summarise + write the generic control tables (one MERGE each) — #4 metadata mechanism
ok=[r for r in results if r.get("ok") or r.get("dry")]
err=[r for r in results if r.get("error")]
run_end_ts=datetime.now(timezone.utc).replace(tzinfo=None)
dur=float((run_end_ts-run_start_ts).total_seconds())
out={"batch_id":RUN_BATCH, "to_reload":len(todo), "reloaded":len(ok), "errors":len(err),
     "skipped_cdc":len(skipped_cdc), "skipped_no_pk":len(skipped_nopk),
     "total_rows":sum(r.get("rows",0) for r in results),
     "error_sample":err[:10], "skipped_no_pk_sample":skipped_nopk[:10]}
print("FULL_RELOAD "+json.dumps(out, default=str))

# Feed the team's canonical Bronze control tables (feedback #4) + the unprefixed batch_watermark that the
# Silver loader actually reads. detail_rows carry per-row schema_name (full reload spans many schemas).
detail_rows=[{"schema_name":r["ss"],"source_table":r["st"],
              "status":("SUCCESS" if r.get("ok") else ("SKIPPED" if r.get("dry") else "FAILED")),
              "inserted_rows":r.get("rows",0),"error_message":r.get("error")} for r in results]
for s in skipped_cdc:
    ss,_,st=s.partition(".")
    detail_rows.append({"schema_name":ss,"source_table":st,"status":"SKIPPED","inserted_rows":0,
                        "error_message":"CDC-owned (stream captures it)"})
for s in skipped_nopk:
    ss,_,st=s.partition(".")
    detail_rows.append({"schema_name":ss,"source_table":st,"status":"SKIPPED","inserted_rows":0,
                        "error_message":"no pk / no bronze_table"})
success_updates=[(TENANT, r["ss"], r["st"], int(RUN_BATCH)) for r in results if r.get("ok")]
exec_id=write_bronze_control_metadata(tenant_id=TENANT, run_batch=RUN_BATCH, run_start_ts=run_start_ts,
            run_end_ts=run_end_ts, pipeline_version="bronze-fullload-1.0", detail_rows=detail_rows,
            success_updates=success_updates, dry=DRY)
out["execution_id"]=exec_id

dbutils.notebook.exit(json.dumps(out, default=str))
