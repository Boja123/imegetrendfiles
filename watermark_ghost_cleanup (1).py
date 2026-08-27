# Databricks notebook source
# MAGIC %md
# MAGIC # Ghost cleanup — delete bronze rows that no longer exist at the source (TEMPORARY TOOL)
# MAGIC
# MAGIC Standalone extract of `incremental_delta_pull.py` phase 2 — **no delta-merge phase**, so it runs in a
# MAGIC fraction of the time when all you need is to reap ghosts. A "ghost" is a row present in
# MAGIC `medallion.bronze.<table>` whose PK no longer exists at the source (deleted there, but the delete
# MAGIC never reached bronze because the table has no CDC / no watermark, or it was deleted during a stall).
# MAGIC
# MAGIC Detected by a **PK anti-join** and deleted from bronze, with the same three guards as the delta pull:
# MAGIC 1. **insert-race guard** — never touch rows newer than the source snapshot: `batch_id < this run`,
# MAGIC    and for a single numeric PK also `pk <= max(source pk)`. Closes the race with live CDC inserts.
# MAGIC 2. **per-table pct fuse** — abort a table's delete if ghosts exceed `ghost_max_pct` of its bronze
# MAGIC    rows (guards against a truncated/failed source read wiping a table).
# MAGIC 3. **scope** — `full` sweeps every active table (pulls ALL source PKs, heavier); `targeted` sweeps
# MAGIC    only tables the reconcile report flags as bronze-heavy (fast, but only as fresh as that report).
# MAGIC
# MAGIC Every doomed PK is logged to `<ghost_report>_deleted_pks` BEFORE the delete (replayable audit trail).
# MAGIC Read-only against the source. `dry_run=true` counts ghosts without deleting.

# COMMAND ----------
import json, time, threading
from datetime import datetime
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
dbutils.widgets.text("schemas", "")            # empty = ALL active registry schemas
dbutils.widgets.text("cdc_schemas", "EmsEvent")  # CDC tables handle their own deletes (delete-events)
dbutils.widgets.dropdown("scope", "watermark", ["watermark", "all"])  # which tables CAN accumulate ghosts
dbutils.widgets.text("registry_table", "control.bronze_table_registry")
dbutils.widgets.dropdown("mode", "full", ["full", "targeted"])
dbutils.widgets.text("ghost_max_pct", "10")    # abort a table's delete if ghosts > this % of its bronze rows
dbutils.widgets.text("reconcile_table", "bronze._seam_reconcile_report")  # feeds 'targeted'
dbutils.widgets.text("ghost_report_table", "bronze._seam_ghost_report")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.text("max_workers", "6")
dbutils.widgets.text("tables", "")  # CSV of source_table names; explicit list sweeps EXACTLY these,
                                    # bypassing the scope=watermark CDC/timestamp exclusion (targets CDC
                                    # tables whose deletes were lost in a stream gap). Empty = normal scope.

SERVER   = dbutils.widgets.get("source_server")
DB       = dbutils.widgets.get("source_database")
TENANT   = int(dbutils.widgets.get("tenant_id"))
SCOPE    = dbutils.widgets.get("secret_scope")
AUTH_MODE = dbutils.widgets.get("auth_mode").strip().lower()
PORT      = dbutils.widgets.get("jdbc_port").strip() or "1433"
JUSER     = dbutils.widgets.get("jdbc_user").strip()
PWKEY     = dbutils.widgets.get("secret_key_password").strip()
TRUST     = dbutils.widgets.get("trust_server_certificate").strip().lower()
SCHEMAS  = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]
TABLES   = {t.strip().lower() for t in dbutils.widgets.get("tables").split(",") if t.strip()}
CDC_SCHEMAS = {x.strip() for x in dbutils.widgets.get("cdc_schemas").split(",") if x.strip()}
GSCOPE   = dbutils.widgets.get("scope")
REGISTRY = dbutils.widgets.get("registry_table")
MODE     = dbutils.widgets.get("mode")
GHOST_PCT = float(dbutils.widgets.get("ghost_max_pct"))
RECON    = dbutils.widgets.get("reconcile_table")
GHOST_REPORT = dbutils.widgets.get("ghost_report_table")
DRY      = dbutils.widgets.get("dry_run") == "true"
WORKERS  = int(dbutils.widgets.get("max_workers"))
BATCH    = int(datetime.now().strftime("%Y%m%d%H%M%S"))
print(f"ghost cleanup: mode={MODE} tenant={TENANT} batch_id={BATCH} dry_run={DRY} pct_fuse={GHOST_PCT}")

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
        r.raise_for_status()
        j = r.json()
        _tok["val"] = j["access_token"]; _tok["exp"] = time.time() + int(j.get("expires_in", 3599))
        return _tok["val"]

URL = f"jdbc:sqlserver://{SERVER}:{PORT};databaseName={DB};encrypt=true;trustServerCertificate={TRUST};loginTimeout=30"

def src_read(sql):
    _r = (spark.read.format("jdbc").option("url", URL).option("query", sql)
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"))
    if AUTH_MODE == "sql":
        _r = _r.option("user", JUSER).option("password", dbutils.secrets.get(SCOPE, PWKEY))
    else:
        _r = _r.option("accessToken", aad_token())
    return _r.load()

# COMMAND ----------
# source metadata: which (schema,table) exist, and which carry a watermark timestamp (ModifiedOn/CreatedOn)
_meta = src_read("SELECT DISTINCT TABLE_SCHEMA s, TABLE_NAME t, COLUMN_NAME c FROM INFORMATION_SCHEMA.COLUMNS").collect()
src_cols = {(r["s"], r["t"]) for r in _meta}
HAS_TS   = {(r["s"], r["t"]) for r in _meta if r["c"].lower() in ("modifiedon", "createdon")}

reg = spark.sql(f"""SELECT source_schema, source_table, bronze_table, pk_columns
                    FROM {REGISTRY} WHERE is_active = true""").collect()
if SCHEMAS:
    reg = [r for r in reg if r["source_schema"] in SCHEMAS]

# Explicit table list: sweep EXACTLY these, regardless of scope/CDC exclusion. Use to reap ghosts on a
# CDC table whose deletes were lost in a stream gap (skip-to-latest / stall / availableNow-no-schedule),
# which the scope=watermark default deliberately skips. Cheap: pulls PKs only for the named tables.
if TABLES:
    reg = [r for r in reg if r["source_table"].lower() in TABLES]
    scope_reg = reg
    print(f"explicit tables=: sweeping {len(scope_reg)} named tables (scope/CDC exclusion bypassed)")

# Only WATERMARK tables can accumulate ghosts, so by default we only sweep those. A watermark table gets
# incremental inserts/updates (by ModifiedOn) but NO delete tracking -> deletes linger as ghosts. The
# other roads self-handle deletes: CDC tables via CDC delete-events, untraceable tables via full-replace.
# Sweeping them would pull all their source PKs for nothing. scope='all' restores the exhaustive sweep.
if not TABLES and GSCOPE == "watermark":
    before = len(reg)
    reg = [r for r in reg if (r["source_schema"], r["source_table"]) in HAS_TS
           and r["source_schema"] not in CDC_SCHEMAS]
    print(f"scope=watermark: {len(reg)} of {before} tables (only tables that can accumulate ghosts)")

if TABLES:
    pass  # scope_reg already set to the explicit named set above
elif MODE == "targeted":
    try:
        heavy = {(r["schema_name"], r["source_table"]) for r in
                 spark.table(RECON).filter("diff < 0").collect()}   # bronze > source NOW
        scope_reg = [r for r in reg if (r["source_schema"], r["source_table"]) in heavy]
        print(f"targeted: {len(scope_reg)} tables flagged bronze-heavy by {RECON}")
    except Exception as e:
        scope_reg = reg
        print(f"targeted requested but {RECON} unreadable ({str(e)[:80]}) — falling back to full sweep")
else:
    scope_reg = reg
    print(f"full: {len(scope_reg)} tables — pulls ALL source PKs, heavier")

# COMMAND ----------
def ghost_sweep(r):
    ss, st, bt = r["source_schema"], r["source_table"], (r["bronze_table"] or "").lower()
    pks = [c.strip() for c in (r["pk_columns"] or "").split(",") if c.strip()]
    res = {"schema_name": ss, "source_table": st, "bronze_table": bt,
           "ghost_rows": 0, "bronze_rows": 0, "status": "OK", "error": None}
    try:
        if not pks:
            res["status"], res["error"] = "SKIPPED", "no pk_columns"; return res
        if (ss, st) not in src_cols:
            res["status"], res["error"] = "SKIPPED", "table not found in source"; return res
        btypes = {f.name.lower(): f.dataType.typeName() for f in spark.table(f"bronze.{bt}").schema.fields}
        bcols = {f.name.lower(): f.name for f in spark.table(f"bronze.{bt}").schema.fields}
        bpks = [bcols[p.lower()] for p in pks if p.lower() in bcols]
        if len(bpks) != len(pks):
            res["status"], res["error"] = "SKIPPED", "pk column missing in bronze"; return res

        # Case-fold string PKs on BOTH sides before joining. SQL Server returns `uniqueidentifier` (GUID)
        # values UPPERCASED over JDBC, while bronze stores them lowercase — so a raw join on a GUID PK
        # matches nothing and flags EVERY row as a ghost (the pct-fuse then aborts the whole table). Join
        # on a normalized key column instead. Numeric PKs are left as-is.
        def norm(colname):
            c = F.col(f"`{colname}`")
            return F.upper(c.cast("string")) if btypes.get(colname.lower()) == "string" else c
        jkeys = [f"_k{i}" for i in range(len(bpks))]

        pk_sql = ", ".join(f"[{p}]" for p in pks)
        src_pk = src_read(f"SELECT {pk_sql} FROM [{ss}].[{st}]")
        src_pk = src_pk.select(*[F.col(c).alias(bcols[c.lower()]) for c in src_pk.columns])
        for jk, p in zip(jkeys, bpks):
            src_pk = src_pk.withColumn(jk, norm(p))
        src_pk = src_pk.select(*jkeys)

        bdf = (spark.table(f"bronze.{bt}")
               .filter(F.col("tenant_id") == TENANT)
               .select(*bpks, F.col("batch_id").cast("string").alias("_b")))
        for jk, p in zip(jkeys, bpks):
            bdf = bdf.withColumn(jk, norm(p))
        res["bronze_rows"] = bdf.count()
        ghosts = (bdf.join(src_pk, jkeys, "left_anti").drop(*jkeys)
                    .filter(F.col("_b") < BATCH))          # never touch rows CDC wrote during/after this run
        if len(bpks) == 1:                                  # numeric-PK insert-race guard (jkeys[0] == raw numeric)
            mx = src_pk.agg(F.max(jkeys[0])).first()[0]
            if mx is not None and str(mx).lstrip("-").isdigit():
                ghosts = ghosts.filter(F.col(f"`{bpks[0]}`") <= mx)
        n = ghosts.count()
        res["ghost_rows"] = n
        if n == 0:
            return res
        if res["bronze_rows"] > 0 and 100.0 * n / res["bronze_rows"] > GHOST_PCT:
            res["status"] = "ABORTED_PCT_FUSE"
            res["error"] = f"ghosts={n} is >{GHOST_PCT}% of bronze rows — refusing; verify source read"
            return res
        if DRY:
            res["status"] = "DRY"; return res
        # audit trail: log the doomed PKs before deleting
        (ghosts.select(*bpks)
            .withColumn("schema_name", F.lit(ss)).withColumn("source_table", F.lit(st))
            .withColumn("tenant_id", F.lit(TENANT)).withColumn("batch_id", F.lit(str(BATCH)))  # audit table's batch_id is string (shared w/ incremental_delta_pull); match it or the append fails DELTA_FAILED_TO_MERGE_FIELDS
            .withColumn("deleted_ts", F.current_timestamp())
            .selectExpr("schema_name", "source_table", "tenant_id", "batch_id", "deleted_ts",
                        f"to_json(struct({', '.join('`'+p+'`' for p in bpks)})) AS pk_json")
            .write.mode("append").saveAsTable(GHOST_REPORT + "_deleted_pks"))
        cond = " AND ".join([f"t.`{p}` = s.`{p}`" for p in bpks] + [f"t.tenant_id = {TENANT}"])
        (DeltaTable.forName(spark, f"bronze.{bt}").alias("t")
            .merge(ghosts.alias("s"), cond).whenMatchedDelete().execute())
        res["status"] = "DELETED"
    except Exception as e:
        res["status"], res["error"] = "FAILED", str(e)[:220]
    return res

with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    ghost_results = list(ex.map(ghost_sweep, scope_reg))

# COMMAND ----------
grep_df = (spark.createDataFrame(ghost_results,
            "schema_name string, source_table string, bronze_table string, "
            "ghost_rows long, bronze_rows long, status string, error string")
           .withColumn("tenant_id", F.lit(TENANT)).withColumn("batch_id", F.lit(BATCH))
           .withColumn("dry_run", F.lit(DRY)).withColumn("run_ts", F.current_timestamp()))
grep_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GHOST_REPORT)

g_del = sum(r["ghost_rows"] for r in ghost_results if r["status"] in ("DELETED", "DRY"))
g_tbl = sum(1 for r in ghost_results if r["status"] in ("DELETED", "DRY") and r["ghost_rows"] > 0)
g_ab  = sum(1 for r in ghost_results if r["status"] == "ABORTED_PCT_FUSE")
g_fail = sum(1 for r in ghost_results if r["status"] == "FAILED")
print(f"ghost sweep: {len(ghost_results)} tables | ghost rows {'found' if DRY else 'deleted'}={g_del:,}"
      f" over {g_tbl} tables | pct-fuse aborts={g_ab} | failed={g_fail}")
display(grep_df.filter("ghost_rows > 0 OR status NOT IN ('OK','DRY','DELETED')")
              .orderBy(F.col("ghost_rows").desc()).limit(60))

dbutils.notebook.exit(json.dumps({
    "batch_id": BATCH, "mode": MODE, "dry_run": DRY, "tables_swept": len(ghost_results),
    "ghost_rows": int(g_del), "ghost_tables": int(g_tbl),
    "pct_fuse_aborts": int(g_ab), "failed": int(g_fail),
    "ghost_report_table": GHOST_REPORT}))
