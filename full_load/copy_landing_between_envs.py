# Databricks notebook source
# MAGIC %md
# MAGIC # Copy a landing `fullload` tree between environments
# MAGIC
# MAGIC Seeds a **fresh** environment's landing zone from an environment that already has an
# MAGIC ADF-produced full-load set, so `full-load/bronze_full_load_ingestion` has something to ingest.
# MAGIC
# MAGIC Blue's landing is produced by ADF (`PL_FullLoad_Master` → `PL_FullLoad_Tenant_Worker`). A new
# MAGIC environment without ADF has an empty landing zone, so Bronze-Full-Load fails at tenant discovery
# MAGIC (`PathNotFound`). This copies the tree verbatim, preserving the layout Bronze-Full-Load walks:
# MAGIC
# MAGIC ```
# MAGIC <base>/tenant_id=<N>/schema_name=<S>/table_name=<T>/batch_id=<B>/*.parquet
# MAGIC ```
# MAGIC
# MAGIC **Both accounts must be readable/writable from this cluster** — set an account key for each in the
# MAGIC cluster's spark_conf (via secrets, never inline).
# MAGIC
# MAGIC `dry_run=true` (the default) only measures: it lists the tree and reports file counts and bytes so
# MAGIC you can size the transfer before committing to it. Copy is idempotent per table dir (it skips a
# MAGIC destination table dir that already has the same file count).

# COMMAND ----------
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

dbutils.widgets.text("src_base", "abfss://landing@eus1pdatpltstg01.dfs.core.windows.net/fullload")
dbutils.widgets.text("dst_base", "abfss://landing@eus1pdatpltstg02.dfs.core.windows.net/fullload")
dbutils.widgets.text("tenant_filter", "")      # e.g. "1" — empty = every tenant found
dbutils.widgets.text("schema_filter", "")      # e.g. "EmsEvent" — empty = every schema
dbutils.widgets.text("max_workers", "16")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.dropdown("overwrite", "false", ["true", "false"])   # re-copy table dirs that already match

SRC = dbutils.widgets.get("src_base").rstrip("/")
DST = dbutils.widgets.get("dst_base").rstrip("/")
TENANTS = {t.strip() for t in dbutils.widgets.get("tenant_filter").split(",") if t.strip()}
SCHEMAS = {s.strip() for s in dbutils.widgets.get("schema_filter").split(",") if s.strip()}
WORKERS = int(dbutils.widgets.get("max_workers") or "16")
DRY = dbutils.widgets.get("dry_run") == "true"
OVERWRITE = dbutils.widgets.get("overwrite") == "true"
print(f"src={SRC}\ndst={DST}\ndry_run={DRY} overwrite={OVERWRITE} workers={WORKERS}")

# COMMAND ----------
def ls(path):
    try:
        return list(dbutils.fs.ls(path))
    except Exception as e:
        if "not exist" in str(e) or "PathNotFound" in str(e):
            return []
        raise

def seg(name):                      # "tenant_id=1/" -> "1"
    return name.rstrip("/").split("=", 1)[1]

# discover the tree: tenant -> schema -> table  (batch dirs come along with the recursive copy)
table_dirs = []          # (tenant, schema, table)
for t in ls(SRC):
    if not t.name.startswith("tenant_id="):
        continue
    tid = seg(t.name)
    if TENANTS and tid not in TENANTS:
        continue
    for s in ls(t.path):
        if not s.name.startswith("schema_name="):
            continue
        sch = seg(s.name)
        if SCHEMAS and sch not in SCHEMAS:
            continue
        for tb in ls(s.path):
            if tb.name.startswith("table_name="):
                table_dirs.append((tid, sch, seg(tb.name)))

print(f"discovered {len(table_dirs)} table dirs under {SRC}")
if not table_dirs:
    dbutils.notebook.exit(json.dumps({"status": "EMPTY", "note": f"no tenant_id=*/schema_name=*/table_name=* under {SRC}"}))

# COMMAND ----------
def walk_files(path):
    """Recursively total (files, bytes) under a path."""
    n = b = 0
    stack = [path]
    while stack:
        for f in ls(stack.pop()):
            if f.name.endswith("/"):
                stack.append(f.path)
            else:
                n += 1; b += f.size
    return n, b

def one(td):
    tid, sch, tbl = td
    rel = f"/tenant_id={tid}/schema_name={sch}/table_name={tbl}"
    src, dst = SRC + rel, DST + rel
    r = {"tenant": tid, "schema": sch, "table": tbl, "files": 0, "bytes": 0,
         "status": "OK", "error": None}
    try:
        n, b = walk_files(src)
        r["files"], r["bytes"] = n, b
        if DRY:
            r["status"] = "MEASURED"; return r
        if not OVERWRITE:
            dn, _ = walk_files(dst)
            if dn == n and n > 0:
                r["status"] = "SKIPPED_SAME"; return r
        dbutils.fs.cp(src, dst, recurse=True)
        r["status"] = "COPIED"
    except Exception as e:
        r["status"], r["error"] = "ERROR", str(e)[:200]
    return r

results = []
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    futs = {ex.submit(one, td): td for td in table_dirs}
    for i, f in enumerate(as_completed(futs), 1):
        results.append(f.result())
        if i % 50 == 0 or i == len(futs):
            done_b = sum(x["bytes"] for x in results)
            print(f"  {i}/{len(futs)} dirs | {done_b/(1024**3):,.1f} GiB seen")

# COMMAND ----------
tot_files = sum(r["files"] for r in results)
tot_bytes = sum(r["bytes"] for r in results)
errs = [r for r in results if r["status"] == "ERROR"]
by_status = {}
for r in results:
    by_status[r["status"]] = by_status.get(r["status"], 0) + 1

print("\n================ LANDING COPY ================")
print(f"  table dirs : {len(results):,}")
print(f"  files      : {tot_files:,}")
print(f"  size       : {tot_bytes/(1024**3):,.2f} GiB")
print(f"  status     : {by_status}")
print(f"  errors     : {len(errs)}")
print("=" * 46)
for r in errs[:10]:
    print("  ERR", r["schema"], r["table"], "-", r["error"])

dbutils.notebook.exit(json.dumps({
    "dry_run": DRY, "table_dirs": len(results), "files": tot_files,
    "gib": round(tot_bytes / (1024 ** 3), 2), "by_status": by_status,
    "errors": len(errs)}))
