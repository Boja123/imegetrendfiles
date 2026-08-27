# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Interim audit-column fill (FireEvent) — the bridge until the source capture instances are rebuilt
# MAGIC
# MAGIC **Source schema: `FireEvent` (fire_nfris pipeline).** Runs as the task AFTER `stage_compact` in the
# MAGIC CDC job. It patches the one thing CDC cannot deliver today: the audit/business-key columns
# MAGIC (`ModifiedOn`, `CreatedOn`, `CreatedBy`, `GlobalIdentifier`, `FormID`, …) that the frozen SQL
# MAGIC Server capture instances omit, so every CDC insert/update lands them as NULL in Bronze
# MAGIC (`fire_nfris_bronze`).
# MAGIC
# MAGIC It **mimics what the rebuilt capture would emit**: for exactly the rows CDC just wrote in this
# MAGIC batch (`batch_id = <this batch>`, `_op <> 'd'`), it looks up those rows in the source **by primary
# MAGIC key** — the PK is the one column CDC *does* carry — and fills the missing columns from the source's
# MAGIC true values. It touches only rows that still have a NULL in a missing column, so it is idempotent
# MAGIC and cheap on a normal 15-minute drain.
# MAGIC
# MAGIC **Registry:** `fire_nfris_control.bronze_table_registry` (280 FireEvent tables, plus
# MAGIC FireInvestigation/FireInvestigationLocation/Resource).
# MAGIC
# MAGIC **Deletes need no handling** — CDC delete events already removed those rows from Bronze in
# MAGIC `stage_compact`, so they are not in this batch's `_op <> 'd'` set and are never queried.
# MAGIC
# MAGIC **Scope of a fill = one batch.** A row deleted at the source *after* CDC captured it but *before*
# MAGIC this fill runs simply won't come back from the source lookup → it stays as-is and the next CDC
# MAGIC delete event reaps it. No ghost is created.
# MAGIC
# MAGIC Retire this notebook once `tools/rebuild_cdc_capture_instances.sql` has been run on the source and
# MAGIC Debezium is cut over to the `_v2` capture instances — from then on CDC carries the columns itself.
# MAGIC Writes `fire_nfris_bronze`; read-only against the source.

# COMMAND ----------

# DBTITLE 1,Cell 2
import json, time, threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import requests
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "medallion")   # green: pass medallion2
CATALOG = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {CATALOG}")   # all `` prefixes stripped below -> resolve against this catalog
from delta.tables import DeltaTable

dbutils.widgets.text("source_server", "eus1pet2smidb02.48e8a7f712c4.database.windows.net")
dbutils.widgets.text("source_database", "Elite_Mississippi")
dbutils.widgets.text("tenant_id", "1")
dbutils.widgets.text("secret_scope", "kv-eus1-p-dat-plt-kvt-01")
# Source auth. Default "aad" = green/blue (data-platform-reader SP token, port 1433).
# "sql" = dev (SQL user/password, custom port). Defaults keep green/blue byte-identical.
dbutils.widgets.text("auth_mode", "aad")
dbutils.widgets.text("jdbc_port", "1433")
dbutils.widgets.text("jdbc_user", "")
dbutils.widgets.text("secret_key_password", "")
dbutils.widgets.text("trust_server_certificate", "true")
dbutils.widgets.text("batch_id", "")               # empty = read from the 'init' task value, else latest compacted batch
dbutils.widgets.text("registry_table", "fire_nfris_control.bronze_table_registry")
dbutils.widgets.text("missing_cols_table", "fire_nfris_bronze._cdc_missing_columns")
dbutils.widgets.text("detail_table", "fire_nfris_control.bronze_pipeline_execution_detail")
dbutils.widgets.text("report_table", "fire_nfris_bronze._cdc_interim_fill_report")
dbutils.widgets.text("bronze_schema", "fire_nfris_bronze")  # schema where bronze tables live
dbutils.widgets.text("pk_chunk", "400")            # source PKs per lookup query
dbutils.widgets.text("max_workers", "8")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"])
# Master on/off. This step backfills the audit columns (ModifiedOn/CreatedOn/CreatedBy/GlobalIdentifier)
# that the source CDC capture instances drop — only needed for point-in-time comparisons on those columns.
# "false" skips it entirely: no source reads, no writes. Default "true" keeps blue unchanged.
dbutils.widgets.dropdown("enabled", "true", ["true", "false"])

SERVER = dbutils.widgets.get("source_server")
DB     = dbutils.widgets.get("source_database")
TENANT = int(dbutils.widgets.get("tenant_id"))
SCOPE  = dbutils.widgets.get("secret_scope")
AUTH_MODE = dbutils.widgets.get("auth_mode").strip().lower()
PORT      = dbutils.widgets.get("jdbc_port").strip() or "1433"
JUSER     = dbutils.widgets.get("jdbc_user").strip()
PWKEY     = dbutils.widgets.get("secret_key_password").strip()
TRUST     = dbutils.widgets.get("trust_server_certificate").strip().lower()
REG    = dbutils.widgets.get("registry_table")
MISS   = dbutils.widgets.get("missing_cols_table")
DETAIL = dbutils.widgets.get("detail_table")
REPORT = dbutils.widgets.get("report_table")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
CHUNK  = int(dbutils.widgets.get("pk_chunk"))
WORKERS = int(dbutils.widgets.get("max_workers"))
DRY    = dbutils.widgets.get("dry_run") == "true"

# Master switch: skip the whole step before any source connection when disabled.
if dbutils.widgets.get("enabled").strip().lower() != "true":
    dbutils.notebook.exit(json.dumps({"status": "disabled",
        "note": "enabled=false — audit fill skipped (only needed for point-in-time comparisons)"}))

# resolve which batch to fill: widget -> upstream 'init' task value -> latest compacted batch for this tenant
BATCH = dbutils.widgets.get("batch_id").strip()
if not BATCH:
    try:
        BATCH = (dbutils.jobs.taskValues.get(taskKey="init", key="batch_id", default="") or "").strip()
    except Exception:
        BATCH = ""
if not BATCH:
    row = spark.sql(f"SELECT max(batch_id) m FROM {DETAIL} WHERE tenant_id={TENANT}").first()
    BATCH = str(row["m"]) if row and row["m"] is not None else ""
if not BATCH:
    dbutils.notebook.exit(json.dumps({"status": "no_batch", "note": "could not resolve a batch_id to fill"}))
BATCH = int(BATCH)
RUN_ID = datetime.now(timezone.utc).strftime("interimfill_%Y%m%d%H%M%S")
print(f"interim audit fill: batch={BATCH} tenant={TENANT} dry_run={DRY} run={RUN_ID}")

# COMMAND ----------

_tok = {"val": None, "exp": 0.0}
_lock = threading.Lock()
def aad_token():
    with _lock:
        if _tok["val"] and _tok["exp"] - time.time() > 300:
            return _tok["val"]
        tid = dbutils.secrets.get(SCOPE, "data-platform-reader-tenant-id")
        cid = dbutils.secrets.get(SCOPE, "data-platform-reader-client-id")
        sec = dbutils.secrets.get(SCOPE, "data-platform-reader-client-secret")
        r = requests.post(f"https://login.microsoftonline.com/{tid}/oauth2/v2.0/token",
            data={"grant_type": "client_credentials", "client_id": cid, "client_secret": sec,
                  "scope": "https://database.windows.net/.default"}, timeout=30)
        r.raise_for_status(); j = r.json()
        _tok["val"] = j["access_token"]; _tok["exp"] = time.time() + int(j.get("expires_in", 3599))
        return _tok["val"]

URL = f"jdbc:sqlserver://{SERVER}:{PORT};databaseName={DB};encrypt=true;trustServerCertificate={TRUST};loginTimeout=30"
def src(sql):
    _r = (spark.read.format("jdbc").option("url", URL).option("query", sql)
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"))
    if AUTH_MODE == "sql":
        _r = _r.option("user", JUSER).option("password", dbutils.secrets.get(SCOPE, PWKEY))
    else:
        _r = _r.option("accessToken", aad_token())
    return _r.load()

def sql_val(v):
    if v is None: return "NULL"
    if isinstance(v, bool): return "1" if v else "0"
    if isinstance(v, (int, float)): return str(v)
    return "'" + str(v).replace("'", "''") + "'"

def pk_predicate(pk_cols, tuples):
    if len(pk_cols) == 1:
        return f"[{pk_cols[0]}] IN (" + ",".join(sql_val(t[0]) for t in tuples) + ")"
    return " OR ".join("(" + " AND ".join(f"[{c}]={sql_val(v)}" for c, v in zip(pk_cols, t)) + ")" for t in tuples)

# COMMAND ----------

# which columns are missing from each table's capture instance, and are NOT blobs, so worth filling
if not spark.catalog.tableExists(MISS):
    print(f"{MISS} not found — no missing-columns map to fill from; skipping (nothing to enrich)")
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "missing_cols_table_absent", "table": MISS}))
BLOB = {"varbinary", "image", "binary", "text", "ntext", "xml", "timestamp"}
miss_rows = spark.table(MISS).collect()
MISSING = {}   # (schema, table) -> [cols]
for r in miss_rows:
    if (r["data_type"] or "").lower() not in BLOB:
        MISSING.setdefault((r["table_schema"], r["table_name"]), []).append(r["column_name"])

# tables this batch actually touched (from the compact detail), joined to registry for pk + bronze name
touched = spark.sql(f"""
    SELECT DISTINCT d.schema_name, d.source_table, r.bronze_table, r.pk_columns
    FROM {DETAIL} d
    JOIN {REG} r ON r.source_schema=d.schema_name AND r.source_table=d.source_table AND r.is_active=true
    WHERE d.tenant_id={TENANT} AND d.batch_id={BATCH} AND d.status='SUCCESS' AND d.inserted_rows > 0
""").collect()
print(f"tables in batch {BATCH}: {len(touched)}")

# COMMAND ----------

# DBTITLE 1,Cell 5
def fill_one(row):
    ss, st = row["schema_name"], row["source_table"]
    bt = (row["bronze_table"] or "").lower()
    pk = [c.strip() for c in (row["pk_columns"] or "").split(",") if c.strip()]
    res = {"schema_name": ss, "source_table": st, "bronze_table": bt,
           "candidate_rows": 0, "filled_rows": 0, "cols_filled": "", "status": "OK", "error": None}
    try:
        cols = MISSING.get((ss, st), [])
        if not pk or not cols:
            res["status"] = "SKIPPED"; res["error"] = "no pk" if not pk else "no missing cols"; return res
        bronze_fqn = f"{BRONZE_SCHEMA}.{bt}"
        bcols = {c.lower(): c for c in spark.table(bronze_fqn).columns}
        pk_b = [bcols[p.lower()] for p in pk if p.lower() in bcols]
        fill = [bcols[c.lower()] for c in cols if c.lower() in bcols]   # bronze-actual names
        if len(pk_b) != len(pk) or not fill:
            res["status"] = "SKIPPED"; res["error"] = "pk/cols not in bronze"; return res
        res["cols_filled"] = ",".join(fill)

        # rows CDC wrote in this batch that STILL have a hole in a missing column (idempotent filter)
        hole = " OR ".join(f"`{c}` IS NULL" for c in fill)
        cand = (spark.table(bronze_fqn)
                .filter((F.col("tenant_id") == TENANT) & (F.col("batch_id") == BATCH)
                        & (F.col("_op") != F.lit("d")) & F.expr(f"({hole})"))
                .select(*[F.col(f"`{c}`") for c in pk_b]).distinct())
        pk_tuples = [tuple(r[c] for c in pk_b) for r in cand.collect()]
        res["candidate_rows"] = len(pk_tuples)
        if not pk_tuples or DRY:
            return res

        # look the rows up in the SOURCE by PK, pull pk + the missing columns, chunked
        sel = ", ".join(f"[{c}]" for c in (pk + cols))
        frames = []
        for i in range(0, len(pk_tuples), CHUNK):
            pred = pk_predicate(pk, pk_tuples[i:i + CHUNK])
            frames.append(src(f"SELECT {sel} FROM [{ss}].[{st}] WHERE {pred}"))
        sdf = frames[0]
        for f in frames[1:]:
            sdf = sdf.unionByName(f)
        # align to bronze column names + tenant key
        ren = {c: bcols[c.lower()] for c in sdf.columns if c.lower() in bcols}
        for old, new in ren.items():
            if old != new: sdf = sdf.withColumnRenamed(old, new)
        sdf = sdf.withColumn("tenant_id", F.lit(TENANT).cast("int"))

        keys = ["tenant_id"] + pk_b
        cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
        upd = {c: f"s.`{c}`" for c in fill}       # fill ONLY the missing columns; never touch CDC-captured ones
        (DeltaTable.forName(spark, bronze_fqn).alias("t")
            .merge(sdf.alias("s"), cond)
            .whenMatchedUpdate(set=upd)
            .execute())
        res["filled_rows"] = len(pk_tuples)
    except Exception as e:
        res["status"], res["error"] = "ERROR", str(e)[:220]
    return res

with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    results = list(ex.map(fill_one, touched))

# COMMAND ----------

rep = (spark.createDataFrame(results,
        "schema_name string, source_table string, bronze_table string, candidate_rows long, "
        "filled_rows long, cols_filled string, status string, error string")
       .withColumn("batch_id", F.lit(BATCH)).withColumn("tenant_id", F.lit(TENANT))
       .withColumn("run_id", F.lit(RUN_ID)).withColumn("run_ts", F.current_timestamp()))
rep.write.mode("append").saveAsTable(REPORT)

t = rep.selectExpr("count(*)", "sum(filled_rows)", "sum(candidate_rows)",
                   "sum(case when status='ERROR' then 1 else 0 end)").first()
print(f"tables={t[0]}  rows_filled={t[1] or 0:,}  candidates={t[2] or 0:,}  errors={t[3]}")
if (t[3] or 0) > 0:
    display(rep.filter("status='ERROR'").limit(20))

dbutils.notebook.exit(json.dumps({
    "batch_id": BATCH, "dry_run": DRY, "tables": int(t[0]),
    "rows_filled": int(t[1] or 0), "candidates": int(t[2] or 0),
    "errors": int(t[3] or 0), "report_table": REPORT}))