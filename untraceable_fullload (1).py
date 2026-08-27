# Databricks notebook source
# MAGIC %md
# MAGIC # Full-load / replacement of the untraceable tables
# MAGIC
# MAGIC The third data-movement road, for the tables the other two can't touch. A table is **untraceable**
# MAGIC when the source has **no timestamp to track it by** (no `ModifiedOn`, no `CreatedOn`, no
# MAGIC `cdc_insert_date`) and it isn't in a CDC schema. There's no way to pull a delta or bound a
# MAGIC point-in-time on these, so the only correct strategy is a **full replacement** from source.
# MAGIC
# MAGIC These are small reference/lookup tables (ICD code lists, GNIS codes, NEMSIS suggested lists — ~65
# MAGIC tables, ~1.2M rows total), so a full reload is cheap and inherently correct: replacement handles
# MAGIC **inserts, updates, and deletes** in one shot (a row gone from source is simply gone after the reload),
# MAGIC so these tables never need the ghost-cleanup road.
# MAGIC
# MAGIC Per table, keyed by whether the registry gives a usable primary key:
# MAGIC - **PK present** → Delta MERGE full-sync: update matched, insert new, and DELETE bronze rows not in
# MAGIC   source (`whenNotMatchedBySourceDelete`). Atomic, no empty window.
# MAGIC - **no PK** → overwrite this tenant's slice: delete `tenant_id=<t>`, insert the fresh source rows.
# MAGIC
# MAGIC Blob columns (varbinary(max)/image/xml/text) are skipped — same as everywhere. Writes bronze,
# MAGIC read-only against source. `dry_run=true` counts only.

# COMMAND ----------
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
dbutils.widgets.text("cdc_schemas", "EmsEvent")           # excluded — CDC owns these
dbutils.widgets.text("registry_table", "control.bronze_table_registry")
dbutils.widgets.text("report_table", "bronze._fullload_untraceable_report")
dbutils.widgets.text("max_workers", "8")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
# Shared run batch_id (yyyymmddHHMMSS). The orchestrator passes ONE id for the whole bronze run so
# every method x client stamps the SAME batch_id that Silver gates on. Empty only for standalone dev
# runs, where we self-mint below. Do NOT self-mint when a value is passed.
dbutils.widgets.text("batch_id", "")

SERVER = dbutils.widgets.get("source_server")
DB     = dbutils.widgets.get("source_database")
TENANT = int(dbutils.widgets.get("tenant_id"))
SCOPE  = dbutils.widgets.get("secret_scope")
AUTH_MODE = dbutils.widgets.get("auth_mode").strip().lower()
PORT      = dbutils.widgets.get("jdbc_port").strip() or "1433"
JUSER     = dbutils.widgets.get("jdbc_user").strip()
PWKEY     = dbutils.widgets.get("secret_key_password").strip()
TRUST     = dbutils.widgets.get("trust_server_certificate").strip().lower()
CDC_SCHEMAS = {x.strip() for x in dbutils.widgets.get("cdc_schemas").split(",") if x.strip()}
REG    = dbutils.widgets.get("registry_table")
REPORT = dbutils.widgets.get("report_table")
WORKERS = int(dbutils.widgets.get("max_workers"))
DRY    = dbutils.widgets.get("dry_run") == "true"
RUN_ID = datetime.now(timezone.utc).strftime("fullload_%Y%m%d%H%M%S")
# Use the passed run batch_id; only self-mint when run standalone (empty widget). This keeps every
# ingestion method in one bronze run under a single batch_id instead of each minting its own now().
_bid = dbutils.widgets.get("batch_id").strip()
BATCH  = int(_bid) if _bid else int(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
print(f"full-load untraceable: tenant={TENANT} dry_run={DRY} run={RUN_ID} batch_id={BATCH}"
      f" (source: {'passed' if _bid else 'self-minted'})")

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

# COMMAND ----------
# source column metadata → which tables have a trackable timestamp (those are NOT untraceable), + blob types
_cols = src("SELECT TABLE_SCHEMA s, TABLE_NAME t, COLUMN_NAME c, DATA_TYPE d, "
            "COLUMNPROPERTY(OBJECT_ID(QUOTENAME(TABLE_SCHEMA)+'.'+QUOTENAME(TABLE_NAME)),COLUMN_NAME,'CharMaxLen') ml "
            "FROM INFORMATION_SCHEMA.COLUMNS").collect()
TRACKABLE = set()
COLS, TYPES = {}, {}
for r in _cols:
    cl = r["c"].lower()
    if cl in ("modifiedon", "createdon", "cdc_insert_date"):
        TRACKABLE.add((r["s"], r["t"]))
    COLS.setdefault((r["s"], r["t"]), []).append(r["c"])
    TYPES[(r["s"], r["t"], cl)] = ((r["d"] or "").lower(), r["ml"])

BLOB = {"varbinary", "image", "binary", "text", "ntext", "xml", "timestamp"}
def is_blob(ss, st, col):
    t, ml = TYPES.get((ss, st, col.lower()), ("", None))
    return t in BLOB or (t in ("varchar", "nvarchar", "varbinary") and ml == -1)

reg = spark.sql(f"""SELECT source_schema, source_table, bronze_table, pk_columns
                    FROM {REG} WHERE is_active=true""").collect()
# untraceable = not a CDC schema AND no trackable timestamp column
targets = [r for r in reg if r["source_schema"] not in CDC_SCHEMAS
           and (r["source_schema"], r["source_table"]) not in TRACKABLE]
print(f"untraceable tables to full-load: {len(targets)}")

# COMMAND ----------
def load_one(r):
    ss, st = r["source_schema"], r["source_table"]
    bt = (r["bronze_table"] or "").lower()
    pk = [c.strip() for c in (r["pk_columns"] or "").split(",") if c.strip()]
    res = {"schema_name": ss, "source_table": st, "bronze_table": bt,
           "method": None, "src_rows": 0, "bronze_before": 0, "status": "OK", "error": None}
    try:
        bronze_fqn = f"bronze.{bt}"
        bcols = {c.lower(): c for c in spark.table(bronze_fqn).columns}
        res["bronze_before"] = int(spark.sql(
            f"SELECT count(*) FROM {bronze_fqn} WHERE tenant_id={TENANT}").first()[0])
        # source columns present in bronze and not blobs
        payload = [c for c in COLS.get((ss, st), [])
                   if c.lower() in bcols and not is_blob(ss, st, c)]
        if not payload:
            res["status"], res["error"] = "SKIPPED", "no non-blob columns in bronze"; return res
        sel = ", ".join(f"[{c}]" for c in payload)
        sdf = src(f"SELECT {sel} FROM [{ss}].[{st}]")
        res["src_rows"] = sdf.count()
        # align to bronze column names + stamp meta
        for c in sdf.columns:
            if c != bcols[c.lower()]:
                sdf = sdf.withColumnRenamed(c, bcols[c.lower()])
        sdf = (sdf.withColumn("tenant_id", F.lit(TENANT).cast("int"))
                  .withColumn("batch_id", F.lit(BATCH).cast("bigint") if "batch_id" in bcols else F.lit(None))
                  .withColumn("ingestion_time", F.current_timestamp())
                  .withColumn("_ingest_date", F.current_date()))
        write_cols = [c for c in sdf.columns if c.lower() in bcols]
        sdf = sdf.select(*[F.col(c) for c in dict.fromkeys(write_cols)])
        if DRY:
            res["method"] = "merge_sync" if pk and all(p.lower() in bcols for p in pk) else "overwrite_tenant"
            res["status"] = "DRY"; return res

        pk_b = [bcols[p.lower()] for p in pk if p.lower() in bcols]
        if pk_b and len(pk_b) == len(pk):
            # atomic full-sync: update/insert/delete-by-source, scoped to this tenant.
            # Explicit colmap over the source-provided columns only (updateAll/insertAll would demand the
            # bronze meta columns _op/run_id/... which the source doesn't have).
            res["method"] = "merge_sync"
            # Canonical-case keys: lowercase STRING key values so the merge matches the full-load's
            # lowercase rows (source/JDBC returns GUIDs UPPERCASE). A raw compare would miss and both
            # INSERT a case-dupe and (via delete-by-source) drop the real lowercase row. No-op otherwise.
            _kt = {f.name.lower(): f.dataType.typeName() for f in sdf.schema.fields}
            for _k in pk_b:
                if _kt.get(_k.lower()) == "string":
                    sdf = sdf.withColumn(_k, F.lower(F.col(_k)))
            colmap = {c: f"s.`{c}`" for c in sdf.columns}
            cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in (["tenant_id"] + pk_b))
            (DeltaTable.forName(spark, bronze_fqn).alias("t")
                .merge(sdf.alias("s"), cond)
                .whenMatchedUpdate(set=colmap)
                .whenNotMatchedInsert(values=colmap)
                .whenNotMatchedBySourceDelete(condition=f"t.tenant_id = {TENANT}")
                .execute())
        else:
            # no usable PK: replace this tenant's slice
            res["method"] = "overwrite_tenant"
            spark.sql(f"DELETE FROM {bronze_fqn} WHERE tenant_id={TENANT}")
            sdf.write.format("delta").mode("append").saveAsTable(bronze_fqn)
    except Exception as e:
        res["status"], res["error"] = "ERROR", str(e)[:220]
    return res

with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    results = list(ex.map(load_one, targets))

# COMMAND ----------
rep = (spark.createDataFrame(results,
        "schema_name string, source_table string, bronze_table string, method string, "
        "src_rows long, bronze_before long, status string, error string")
       .withColumn("tenant_id", F.lit(TENANT)).withColumn("batch_id", F.lit(BATCH))
       .withColumn("run_id", F.lit(RUN_ID)).withColumn("run_ts", F.current_timestamp()))
rep.write.mode("append").saveAsTable(REPORT)

t = rep.selectExpr("count(*)", "sum(src_rows)",
                   "sum(case when status='ERROR' then 1 else 0 end)",
                   "sum(case when status='SKIPPED' then 1 else 0 end)").first()
print(f"tables={t[0]}  src_rows={t[1] or 0:,}  errors={t[2]}  skipped={t[3]}")
if (t[2] or 0) > 0:
    display(rep.filter("status='ERROR'").limit(20))

dbutils.notebook.exit(json.dumps({
    "run_id": RUN_ID, "dry_run": DRY, "tables": int(t[0]), "src_rows": int(t[1] or 0),
    "errors": int(t[2] or 0), "skipped": int(t[3] or 0), "report_table": REPORT}))
