# Debezium CDC Fleet

Deterministic generator + deploy flow for Debezium Server across multiple VMs.
Streams SQL MI CDC events to Azure Event Hub for downstream Databricks processing.

## Architecture

```
tenants.csv + tiers.json
        ↓
generate_configs.py  →  generated/<vm>/docker-compose.yml
        ↓
deploy.sh            →  rsync to VM + docker compose up -d
        ↓
SQL MI (Elite_*)  →  Debezium Server (Docker)  →  Event Hub (debezium-cdc)  →  Databricks
```

## Repository Structure

```
debezium/
├── tenants.csv              # Source of truth — one row per tenant DB
├── tiers.json               # Per-tier defaults (DBs/container, poll interval)
├── .gitignore               # Excludes .env, generated/, *.dat
├── README.md                # This file
├── dev/                     # Dev environment CI/CD config (GitHub Actions managed)
│   ├── docker-compose.yml
│   ├── config/
│   │   └── application.properties
│   └── .env.example
└── scripts/
    ├── generate_configs.py  # Generates docker-compose.yml per VM
    └── deploy.sh            # Deploys configs to target VM
```

## Tenant Management

### tenants.csv columns

| Column | Description |
|---|---|
| `tenant_id` | Unique tenant identifier |
| `server_name` | SQL MI public endpoint FQDN |
| `port_number` | Port (3342 for SQL MI public endpoint) |
| `user_name` | SQL login with CDC read access |
| `password` | SQL login password (stored in Key Vault — use placeholder here) |
| `dbname` | Database name on the SQL MI |
| `include_list` | Table filter regex e.g. `EmsEvent\..*` |
| `tier` | `tier1_heavy`, `tier2_medium`, or `tier3_light` |
| `vm_hostname` | Target VM hostname e.g. `eus1-d-elt-deb-vm-02` |

### Adding a tenant

1. Add a row to `tenants.csv` with the correct tier and vm_hostname
2. Run the generator (see below)
3. Run deploy.sh targeting the VM

### Removing a tenant

1. Remove the row from `tenants.csv`
2. Run the generator — the container packing will change
3. Run deploy.sh with `--remove-orphans` (already in deploy.sh) — Docker removes containers no longer in the compose file

### Active Table List as source of truth (`include_list`)

`include_list` is governed by the SQL Server **Active Table List** —
`Elite_ADF_MetaData_DB.dbo.cdc_table_control` (the same table the ADF pipelines query:
`WHERE is_active = 1 AND client_id = <tenant>`) — **not** the Bronze table registry. Only
active tables should publish to Event Hub.

`scripts/sync_active_tables.py` materialises this into the `include_list` column. It is
**fail-closed** and **dry-run by default** — it refuses to write a list that would silently
shrink capture (source error, 0 active tables, fewer than `--min-tables`, or 0 rows in
`--require-schema`), and it never restarts the connector (that is a separate gated step —
see *Known Issues* re: schema-history corruption).

```bash
cd debezium/scripts
DB_PASSWORD=*** python3 sync_active_tables.py --tenants ../tenants.csv \
    --require-schema EmsEvent --min-tables 1            # verify (writes nothing)
# ...then, once verified, add --apply, and run generate_configs.py + deploy.sh
```
> Reachability: the SQL MI is only reachable from the Databricks subnet / the Debezium VM,
> so run this there. Password comes from `$DB_PASSWORD` (same Key Vault secret deploy.sh uses).

> ✅ **Status (2026-07-01): connector NARROWED at the source + deployed.**
> `cdc_table_control.is_active` is populated for the **634 active EmsEvent tables** (the source of
> truth), and `dev/config/application.properties` now sets `table.include.list` to that explicit
> 634-table list (was `EmsEvent\..*`). Deployed to the dev `debezium` container on
> `eus1-d-elt-deb-vm-02` and verified: 634 tables in the include_list, container healthy, the
> CDC-enabled-but-inactive tables (e.g. `Guarantor`, `ParamedicIncidentReport`) now EXCLUDED,
> active tables (e.g. `Incident`) kept.
>
> **Decision: filter at the source (do not publish inactive tables).** An earlier call kept the
> static `EmsEvent\..*` on the grounds that the 25 inactive tables are low-volume (~33 msg/day) and
> the bronze fan-out already drops them. That reasoning holds for *one* client but not at fleet scale
> (many clients × wider bursts): capturing a whole schema only to discard most of it downstream wastes
> Event Hub throughput and connector work where it is scarcest. The correct place to filter is the
> source. A Debezium restart to apply the list is routine (the earlier "schema-history crash-loop"
> concern was a one-off corrupted state file, not a normal restart outcome).
>
> **Follow-up (not yet done): make it reactive.** `sync_active_tables.py` is the fail-closed derive
> engine (`cdc_table_control.is_active` → `include_list`); wire it to regenerate + redeploy on a
> control-table change so the connector list tracks `is_active` automatically instead of by hand.

---

## Tier Configuration (tiers.json)

| Tier | DBs/container | Poll interval | Use case |
|---|---|---|---|
| `tier1_heavy` | 10 | 250ms | Large agencies, high CDC rate |
| `tier2_medium` | 25 | 500ms | Mid-size agencies |
| `tier3_light` | 75 | 1000ms | Small/inactive agencies |

Heartbeat interval is jittered ±15% per container to avoid synchronized SQL MI spikes.

---

## Generating Configs

### Prerequisites
- Python 3.x installed
- `tenants.csv` and `tiers.json` up to date

### Run generator

```bash
cd debezium/scripts
python generate_configs.py \
  --tenants ../tenants.csv \
  --tiers ../tiers.json \
  --output ./generated \
  --eventhub-hub debezium-cdc
```

### Output

```
scripts/generated/
└── <vm_hostname>/
    ├── docker-compose.yml   # One service per container pack
    └── .env.example         # Documents required secrets
```

---

## Deploying to a VM

### Prerequisites
- Azure CLI installed and logged in (`az login`)
- SSH key for target VM available
- `az account set --subscription 3f0c52f4-890d-4a41-8c97-ac16618acff3`

### Run deploy

```bash
cd debezium/scripts

# Using existing az login session (manual deploy)
export SKIP_LOGIN=true
export SSH_KEY="/path/to/vm-key.pem"
./deploy.sh <vm_hostname> <vm_ip> ./generated/<vm_hostname>

# Example
export SKIP_LOGIN=true
export SSH_KEY="/c/Users/Ptaveen Teja/Downloads/eus1-d-elt-deb-vm-02-key.pem"
./deploy.sh eus1-d-elt-deb-vm-02 20.25.16.1 ./generated/eus1-d-elt-deb-vm-02
```

### What deploy.sh does

1. Authenticates to Azure (SP or existing session)
2. Fetches secrets from Key Vault (`kv-imgtrend-dev-eus`)
3. Creates data directories on VM (`/opt/debezium/data/<container>/`)
4. Copies `docker-compose.yml` to VM (`/opt/debezium/<vm_hostname>/`)
5. Writes `.env` file on VM with secrets (chmod 600, never committed)
6. Runs `docker compose up -d --remove-orphans`
7. Health checks each container via `/q/health`

### Key Vault secrets required

| Secret name | Description |
|---|---|
| `debezium-db-password` | SQL MI password for PipelineUser |
| `eventhub-connection-string` | Event Hub SAS connection string |

---

## VM Details

### Dev

| VM | IP | SSH Key | Containers |
|---|---|---|---|
| `eus1-d-elt-deb-vm-02` | `20.25.16.1` | `eus1-d-elt-deb-vm-02-key.pem` | `debezium-tier3-light-eus1-d-elt-deb-vm-02-001` |

### Data directory structure on VM

```
/opt/debezium/
├── <vm_hostname>/
│   ├── docker-compose.yml   # Deployed by deploy.sh
│   └── .env                 # Written by deploy.sh — NOT in git
└── data/
    └── <container_name>/    # offsets.dat + schema_history.dat
```

---

## Verifying Deployment

### Check containers running

```bash
ssh -i <key.pem> azureuser@<vm_ip> "docker ps"
```

### Check logs

```bash
ssh -i <key.pem> azureuser@<vm_ip> \
  "docker logs <container_name> --tail=50"
```

### Check Event Hub receiving messages

```bash
az monitor metrics list \
  --resource "eus1delteh01" \
  --resource-group "eus1-a-elt-app-rgp-02" \
  --resource-type "Microsoft.EventHub/namespaces" \
  --resource-namespace "Microsoft.EventHub" \
  --metric "IncomingMessages" \
  --interval PT5M \
  --query "value[0].timeseries[0].data[-6:]" -o table
```

---

## GitHub Actions CI/CD (Dev)

The workflow `.github/workflows/debezium-dev-deploy.yml` triggers on push to `debezium/dev/**` and deploys updated config to `eus1-d-elt-deb-vm-02` using the self-hosted GitHub Actions runner installed on the VM.

This CI/CD manages the **single dev container** (`debezium` — the original setup).
The **fleet containers** (generated by generate_configs.py) are deployed manually via deploy.sh for now.

---

## Known Issues & Notes

- `.env` file is written on VM by deploy.sh — **never commit it**
- `scripts/generated/` is gitignored — regenerate locally before each deploy
- Container runs as UID 185 (jboss) — data directories need `chmod 777`
- `Elite_Demo` and `Elite_Demo2` not yet tested — CDC agent status on those DBs needs verification with Mohit
- TODO: Switch from `scp` to `rsync` in deploy.sh when scaling to multiple VMs
- TODO: Integrate deploy.sh into GitHub Actions using `dev-data-platform-cicd-sp` (SP secret expired — request renewal from Tyler)
