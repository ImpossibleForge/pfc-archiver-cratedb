# pfc-archiver-cratedb — Autonomous archive daemon for CrateDB

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-v3.4-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)
[![Version](https://img.shields.io/badge/pfc--archiver--cratedb-v0.2.1-brightgreen.svg)](https://github.com/ImpossibleForge/pfc-archiver-cratedb/releases)

A standalone daemon that runs alongside CrateDB, watches for data older than a configurable retention window, compresses it to PFC format, and writes it to local storage or S3.

**Runs as a sidecar or cron job — no schema changes, no plugins, no database modifications.**

---

## How it works

Every `interval_seconds` (default: 3600), pfc-archiver-cratedb runs one archive cycle:

```
SCAN  ->  EXPORT  ->  COMPRESS  ->  UPLOAD  ->  VERIFY  ->  (optional DELETE)  ->  LOG
```

1. **SCAN** — compute which time partitions are older than `retention_days`
2. **EXPORT** — stream rows via PostgreSQL wire protocol in `partition_days`-sized chunks
3. **COMPRESS** — pipe through `pfc_jsonl compress` → `.pfc` + `.pfc.bidx` + `.pfc.idx`
4. **UPLOAD** — write to `output_dir` (local path or `s3://bucket/prefix/`)
5. **VERIFY** — decompress and count rows; must match exported count exactly
6. **DELETE** _(optional)_ — `DELETE WHERE ts >= from AND ts < to` (only if `delete_after_archive = true`)
7. **LOG** — write a JSON run log to `log_dir`

---

## Install

```bash
pip install pfc-archiver-cratedb

# With S3 output support
pip install "pfc-archiver-cratedb[s3]"

# Or from source
git clone https://github.com/ImpossibleForge/pfc-archiver-cratedb
pip install psycopg2-binary
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

> **License note:** `pfc_jsonl` is free for personal and open-source use. Commercial use requires a written license — see [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl).

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

```toml
[db]
host      = "localhost"
port      = 5432
user      = "crate"
password  = ""
dbname    = "doc"
schema    = "doc"
table     = "logs"
ts_column = "ts"
batch_size = 10000

[archive]
retention_days       = 30
partition_days       = 1
output_dir           = "./archives/"   # local path or s3://bucket/prefix/
verify               = true
delete_after_archive = false
log_dir              = "./archive_logs/"

[daemon]
interval_seconds = 3600
```

See `config/cratedb.toml` for a fully annotated example.

---

## Output format

Each archive cycle produces:

```
<table>_<YYYYMMDD>_<YYYYMMDD>.pfc
<table>_<YYYYMMDD>_<YYYYMMDD>.pfc.bidx
<table>_<YYYYMMDD>_<YYYYMMDD>.pfc.idx
```

---

## Log format

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
  "output":      "./archives/logs_20260301_20260302.pfc",
  "verified":    true,
  "deleted":     false,
  "status":      "ok"
}
```

---

## Run as a systemd service

```ini
[Unit]
Description=pfc-archiver-cratedb — PFC archive daemon
After=network.target

[Service]
Type=simple
User=pfc
WorkingDirectory=/opt/pfc-archiver-cratedb
ExecStart=/usr/bin/python3 /opt/pfc-archiver-cratedb/pfc_archiver.py --config /etc/pfc-archiver/cratedb.toml
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

---

## Querying cold archives

```sql
INSTALL pfc FROM community;
LOAD pfc;
LOAD json;

-- Time-window query (only decompresses the relevant blocks)
SELECT *
FROM read_pfc_jsonl(
    './archives/logs_20260301_20260302.pfc',
    ts_from = epoch(TIMESTAMPTZ '2026-03-01 14:00:00+00'),
    ts_to   = epoch(TIMESTAMPTZ '2026-03-01 15:00:00+00')
);
```

---

## Part of the PFC Ecosystem

**[→ View all PFC tools & integrations](https://github.com/ImpossibleForge/pfc-jsonl#ecosystem)**

| Direct integration | Why |
|---|---|
| [pfc-export-cratedb](https://github.com/ImpossibleForge/pfc-export-cratedb) | Same DB, different mode — exporter is one-shot CLI; archiver runs as a continuous daemon |
| [pfc-archiver-questdb](https://github.com/ImpossibleForge/pfc-archiver-questdb) | Same concept for QuestDB |

---

## Disclaimer

pfc-archiver-cratedb is an independent open-source project and is not affiliated with, endorsed by, or associated with Crate.io GmbH or the CrateDB project.

---

## License

pfc-archiver-cratedb (this repository) is released under the MIT License — see [LICENSE](LICENSE).

The PFC-JSONL binary (`pfc_jsonl`) is proprietary software — free for personal and open-source use. Commercial use requires a license: [info@impossibleforge.com](mailto:info@impossibleforge.com)
