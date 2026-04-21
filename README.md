# pfc-archiver-cratedb — Autonomous archive daemon for CrateDB

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-v3.4-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)
[![Version](https://img.shields.io/badge/pfc--archiver--cratedb-v0.1.0-brightgreen.svg)](https://github.com/ImpossibleForge/pfc-archiver-cratedb/releases)

A standalone daemon that runs alongside CrateDB, watches for data older than a configurable retention window, compresses it to PFC format, and writes it to local storage or S3 — automatically.

**Runs as a sidecar or cron job — no schema changes, no plugins, no CrateDB modifications.**

---

## How it works

Every `interval_seconds` (default: 3600), pfc-archiver-cratedb runs one archive cycle:

```
SCAN  ->  EXPORT  ->  COMPRESS  ->  UPLOAD  ->  VERIFY  ->  (optional DELETE)  ->  LOG
```

1. **SCAN** — compute which time partitions in CrateDB are older than `retention_days`
2. **EXPORT** — read rows in `partition_days`-sized chunks via PostgreSQL wire protocol
3. **COMPRESS** — pipe through `pfc_jsonl compress` → `.pfc` + `.pfc.bidx` + `.pfc.idx`
4. **UPLOAD** — write to `output_dir` (local path or `s3://bucket/prefix/`)
5. **VERIFY** — decompress and count rows; must match exported count exactly
6. **DELETE** _(optional)_ — `DELETE WHERE ts >= from AND ts < to` (only if `delete_after_archive = true`)
7. **LOG** — write a JSON run log to `log_dir`

---

## Supported databases

| Database | Protocol | Default port |
|----------|----------|-------------|
| CrateDB | PostgreSQL wire (psycopg2) | 5432 |

---

## Install

```bash
pip install pfc-archiver-cratedb

# Or from source
git clone https://github.com/ImpossibleForge/pfc-archiver-cratedb
cd pfc-archiver-cratedb
pip install -r requirements.txt
```

**The `pfc_jsonl` binary must be installed:**

```bash
# Linux x64:
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS (Apple Silicon M1–M4):
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

> **License note:** This tool requires the `pfc_jsonl` binary. `pfc_jsonl` is free for personal and open-source use — commercial use requires a separate license. See [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) for details.

> **macOS Intel (x64):** Binary coming soon.
> **Windows:** No native binary. Use WSL2 or a Linux machine.

**Python dependency for CrateDB:**

```bash
pip install psycopg2-binary
```

---

## Quick start

```bash
# 1. Copy the example config
cp config/cratedb.toml my_config.toml

# 2. Edit the config
nano my_config.toml

# 3. Dry run (no writes, prints what would be archived)
python pfc_archiver.py --config my_config.toml --dry-run

# 4. Archive once and exit
python pfc_archiver.py --config my_config.toml --once

# 5. Run as a daemon (loops every interval_seconds)
python pfc_archiver.py --config my_config.toml
```

---

## Configuration

All config is TOML. A complete example is in `config/cratedb.toml`.

```toml
[db]
db_type   = "cratedb"
host      = "localhost"
port      = 5432
user      = "crate"
password  = ""
database  = "doc"
schema    = "doc"
table     = "logs"
ts_column = "ts"             # your timestamp column
batch_size = 10000

[archive]
retention_days       = 30         # archive data older than this many days
partition_days       = 1          # export this many days per archive file
output_dir           = "./archives/"   # local path or s3://bucket/prefix/
verify               = true       # decompress + count rows after each archive
delete_after_archive = false      # DELETE rows from CrateDB after successful verify
log_dir              = "./archive_logs/"

[daemon]
interval_seconds = 3600           # how often to run (in daemon mode)
```

---

## Output format

Each archive cycle produces files named:

```
<schema>__<table>__<YYYYMMDD>__<YYYYMMDD>.pfc
<schema>__<table>__<YYYYMMDD>__<YYYYMMDD>.pfc.bidx
<schema>__<table>__<YYYYMMDD>__<YYYYMMDD>.pfc.idx
```

The `.pfc` file is a PFC-JSONL archive. The `.bidx` and `.idx` files are block indexes that let DuckDB decompress only the relevant time window — without reading the whole file.

---

## Log format

Each completed cycle appends a JSON entry to `<log_dir>/archive_<YYYYMMDD>.log`:

```json
{
  "ts":          "2026-04-14T18:00:00",
  "db":          "cratedb://localhost:5432/doc",
  "table":       "logs",
  "from":        "2026-03-01T00:00:00",
  "to":          "2026-03-02T00:00:00",
  "rows":        248721,
  "jsonl_mb":    42.3,
  "pfc_mb":      2.5,
  "ratio_pct":   5.9,
  "output":      "./archives/doc__logs__20260301__20260302.pfc",
  "verified":    true,
  "deleted":     false,
  "status":      "ok"
}
```

---

## Run as a systemd service

```ini
[Unit]
Description=pfc-archiver-cratedb — PFC archive daemon for CrateDB
After=network.target

[Service]
Type=simple
User=pfc
WorkingDirectory=/opt/pfc-archiver-cratedb
ExecStart=/usr/bin/python3 /opt/pfc-archiver-cratedb/pfc_archiver.py --config /etc/pfc-archiver-cratedb/cratedb.toml
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable pfc-archiver-cratedb
sudo systemctl start pfc-archiver-cratedb
sudo journalctl -u pfc-archiver-cratedb -f
```

---

## Run as a Docker sidecar

```yaml
# docker-compose.yml
services:
  cratedb:
    image: crate:latest
    ports: ["4200:4200", "5432:5432"]

  pfc-archiver-cratedb:
    image: ghcr.io/impossibleforge/pfc-archiver-cratedb:latest
    volumes:
      - ./config/cratedb.toml:/etc/pfc-archiver-cratedb/config.toml
      - ./archives:/archives
      - ./archive_logs:/logs
    environment:
      - PFC_CONFIG=/etc/pfc-archiver-cratedb/config.toml
    depends_on: [cratedb]
```

---

## Deleting archived data

`delete_after_archive = false` by default — pfc-archiver-cratedb never modifies your CrateDB without explicit opt-in.

After confirming your archives are accessible via DuckDB, set `delete_after_archive = true` and restart. Only partitions that pass the row-count verify step will be deleted.

```sql
-- Manual deletion if needed
DELETE FROM logs WHERE ts >= '2026-03-01' AND ts < '2026-03-02'
```

---

## Querying cold archives

Once archived, your `.pfc` files are queryable directly from DuckDB — alongside live CrateDB data:

```sql
INSTALL pfc FROM community;
LOAD pfc;
LOAD json;

-- Scan a single archive
SELECT *
FROM read_pfc_jsonl('./archives/doc__logs__20260301__20260302.pfc')
LIMIT 100;

-- Time-window query (only decompresses the relevant blocks)
SELECT *
FROM read_pfc_jsonl(
    './archives/doc__logs__20260301__20260302.pfc',
    ts_from = epoch(TIMESTAMPTZ '2026-03-01 14:00:00+00'),
    ts_to   = epoch(TIMESTAMPTZ '2026-03-01 15:00:00+00')
);

-- Hybrid: cold PFC archives + live CrateDB in one query
-- See: https://github.com/ImpossibleForge/pfc-migrate/blob/main/examples/cratedb_archive_explorer.py
```

---

## Related Projects

| Project | Description |
|---------|-------------|
| [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) | Core binary — compress, decompress, query |
| [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) | DuckDB Community Extension (`INSTALL pfc FROM community`) |
| [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) | One-shot CrateDB export and JSONL archive conversion |
| [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) | Fluent Bit -> PFC forwarder for live pipelines |
| [pfc-vector](https://github.com/ImpossibleForge/pfc-vector) | High-performance Rust ingest daemon for Vector.dev and Telegraf |

---

## License

MIT — see [LICENSE](https://github.com/ImpossibleForge/pfc-archiver-cratedb/blob/main/LICENSE).

*Built by [ImpossibleForge](https://github.com/ImpossibleForge)*
