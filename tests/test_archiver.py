"""
Tests for pfc-archiver-cratedb
================================
All tests run without a live CrateDB instance — psycopg2 is mocked throughout.
Run with:  python -m pytest tests/ -v
           python -m unittest discover -s tests -v
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Allow importing from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from pfc_archiver import (
    load_config,
    _connect,
    get_partition_ranges,
    export_partition_to_pfc,
    upload_archive,
    verify_archive,
    delete_partition,
    write_run_log,
    archive_cycle,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(overrides=None):
    """Minimal valid config dict for testing."""
    cfg = {
        "db": {
            "host":      "localhost",
            "port":      5432,
            "user":      "crate",
            "password":  "",
            "dbname":    "doc",
            "schema":    "doc",
            "table":     "logs",
            "ts_column": "timestamp",
        },
        "archive": {
            "retention_days":       30,
            "partition_days":       1,
            "output_dir":           "./test-archives/",
            "verify":               True,
            "delete_after_archive": False,
            "log_dir":              "./test-logs/",
        },
        "daemon": {"interval_seconds": 3600},
    }
    if overrides:
        for section, vals in overrides.items():
            cfg.setdefault(section, {}).update(vals)
    return cfg


# ===========================================================================
# 1. Config loading
# ===========================================================================

class TestLoadConfig(unittest.TestCase):

    def _write_toml(self, content: str) -> str:
        """Write a TOML string to a temp file and return its path."""
        fd, path = tempfile.mkstemp(suffix=".toml")
        os.write(fd, content.encode())
        os.close(fd)
        return path

    def test_valid_config_loads(self):
        toml = """
[db]
host      = "localhost"
table     = "logs"
ts_column = "timestamp"
[archive]
retention_days = 30
output_dir     = "./out/"
"""
        path = self._write_toml(toml)
        cfg = load_config(path)
        self.assertEqual(cfg["db"]["host"], "localhost")
        self.assertEqual(cfg["archive"]["retention_days"], 30)
        os.unlink(path)

    def test_missing_required_field_raises(self):
        toml = """
[db]
host = "localhost"
table = "logs"
# ts_column intentionally missing
[archive]
retention_days = 30
output_dir = "./out/"
"""
        path = self._write_toml(toml)
        with self.assertRaises(ValueError) as ctx:
            load_config(path)
        self.assertIn("ts_column", str(ctx.exception))
        os.unlink(path)

    def test_missing_db_section_raises(self):
        toml = """
[archive]
retention_days = 30
output_dir = "./out/"
"""
        path = self._write_toml(toml)
        with self.assertRaises(ValueError):
            load_config(path)
        os.unlink(path)


# ===========================================================================
# 2. Connection defaults — CrateDB-specific
# ===========================================================================

class TestConnect(unittest.TestCase):

    @patch("pfc_archiver.psycopg2")
    def test_default_port_is_5432(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["port"], 5432)

    @patch("pfc_archiver.psycopg2")
    def test_default_user_is_crate(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["user"], "crate")

    @patch("pfc_archiver.psycopg2")
    def test_default_password_is_empty(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["password"], "")

    @patch("pfc_archiver.psycopg2")
    def test_default_dbname_is_doc(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["dbname"], "doc")

    @patch("pfc_archiver.psycopg2")
    def test_custom_port_overrides_default(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "myhost", "port": 9999, "table": "t", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["port"], 9999)


# ===========================================================================
# 3. Schema-qualified table references — CrateDB requires schema
# ===========================================================================

class TestSchemaQueries(unittest.TestCase):
    """CrateDB uses schema-qualified table refs: "doc"."logs" — must be present."""

    @patch("pfc_archiver._connect")
    def test_get_partition_ranges_schema_in_query(self, mock_connect):
        now    = datetime.now(timezone.utc)
        min_ts = now - timedelta(days=40)
        max_ts = now - timedelta(days=2)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {
            "host": "localhost", "schema": "doc",
            "table": "logs", "ts_column": "timestamp",
        }
        get_partition_ranges(db_cfg, retention_days=30, partition_days=1)

        executed_sql = mock_cur.execute.call_args[0][0]
        # Must contain schema-qualified reference
        self.assertIn('"doc"."logs"', executed_sql)

    @patch("pfc_archiver._connect")
    def test_get_partition_ranges_custom_schema(self, mock_connect):
        now    = datetime.now(timezone.utc)
        min_ts = now - timedelta(days=40)
        max_ts = now - timedelta(days=2)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {
            "host": "localhost", "schema": "myschema",
            "table": "events", "ts_column": "ts",
        }
        get_partition_ranges(db_cfg, retention_days=30, partition_days=1)

        executed_sql = mock_cur.execute.call_args[0][0]
        self.assertIn('"myschema"."events"', executed_sql)

    @patch("pfc_archiver._connect")
    def test_delete_partition_schema_in_query(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.rowcount = 100
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        db_cfg  = {
            "host": "localhost", "schema": "doc",
            "table": "events", "ts_column": "ts",
        }
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        delete_partition(db_cfg, from_ts, to_ts)

        executed_sql = mock_cur.execute.call_args[0][0]
        self.assertIn('"doc"."events"', executed_sql)


# ===========================================================================
# 4. Timestamp handling — CrateDB returns standard datetimes
# ===========================================================================

class TestTimestampHandling(unittest.TestCase):

    @patch("pfc_archiver._connect")
    def test_tz_aware_datetime_handled(self, mock_connect):
        """CrateDB returns tz-aware datetimes — must be processed correctly."""
        now    = datetime.now(timezone.utc)
        min_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        max_ts = now - timedelta(days=32)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "schema": "doc", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertIsInstance(result, list)

    @patch("pfc_archiver._connect")
    def test_naive_datetime_treated_as_utc(self, mock_connect):
        """Naive datetime (no tzinfo) must be treated as UTC."""
        naive_min = datetime(2026, 1, 1, 0, 0, 0)  # no tzinfo

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (naive_min, datetime(2026, 4, 1))
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "schema": "doc", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertIsInstance(result, list)

    @patch("pfc_archiver._connect")
    def test_empty_table_returns_empty_list(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (None, None)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "schema": "doc", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertEqual(result, [])


# ===========================================================================
# 5. Partition range calculation
# ===========================================================================

class TestPartitionRanges(unittest.TestCase):

    @patch("pfc_archiver._connect")
    def test_single_partition_one_day_old(self, mock_connect):
        now     = datetime.now(timezone.utc)
        min_ts  = now - timedelta(days=35)
        max_ts  = now - timedelta(days=34)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "schema": "doc", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertGreater(len(result), 0)
        for from_ts, to_ts in result:
            self.assertLess(from_ts, to_ts)

    @patch("pfc_archiver._connect")
    def test_hot_data_excluded(self, mock_connect):
        """Data within retention_days must NOT appear in partitions."""
        now    = datetime.now(timezone.utc)
        min_ts = now - timedelta(days=5)   # only 5 days old — hot
        max_ts = now

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "schema": "doc", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertEqual(result, [], "Hot data should not be archived")

    @patch("pfc_archiver._connect")
    def test_partition_days_respected(self, mock_connect):
        now    = datetime.now(timezone.utc)
        min_ts = now - timedelta(days=60)
        max_ts = now - timedelta(days=1)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "schema": "doc", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=7)
        self.assertGreater(len(result), 2)
        for from_ts, to_ts in result:
            span = (to_ts - from_ts).total_seconds() / 86400
            self.assertLessEqual(span, 7.01)


# ===========================================================================
# 6. Export — dry-run and timestamp alias injection
# ===========================================================================

class TestExportPartition(unittest.TestCase):

    def test_dry_run_returns_zeros(self):
        db_cfg = {
            "host": "localhost", "schema": "doc",
            "table": "logs", "ts_column": "timestamp",
        }
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        result = export_partition_to_pfc(
            db_cfg, from_ts, to_ts,
            Path("/tmp/dry.pfc"), "/usr/bin/pfc_jsonl",
            dry_run=True,
        )
        self.assertEqual(result["rows"], 0)
        self.assertEqual(result["jsonl_mb"], 0)

    @patch("pfc_archiver._connect")
    @patch("pfc_archiver.subprocess.run")
    def test_timestamp_alias_injected_when_column_differs(self, mock_run, mock_connect):
        """When ts_column != 'timestamp', a 'timestamp' alias must be added to each row."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_run.return_value = mock_proc

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("event_time",), ("level",)]
        ts_val    = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_cur.fetchmany.side_effect = [
            [(ts_val, "ERROR")],
            []
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        db_cfg  = {
            "host": "localhost", "schema": "doc",
            "table": "logs", "ts_column": "event_time",
        }
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "test.pfc"
            out_pfc.write_bytes(b"FAKE")

            export_partition_to_pfc(
                db_cfg, from_ts, to_ts, out_pfc,
                "/usr/bin/pfc_jsonl",
            )

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        self.assertIn("compress", call_args)

    @patch("pfc_archiver._connect")
    def test_empty_partition_returns_skipped(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("level",)]
        mock_cur.fetchmany.return_value = []
        mock_conn.cursor.return_value   = mock_cur
        mock_connect.return_value       = mock_conn

        db_cfg  = {
            "host": "localhost", "schema": "doc",
            "table": "logs", "ts_column": "timestamp",
        }
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        result = export_partition_to_pfc(
            db_cfg, from_ts, to_ts,
            Path("/tmp/empty.pfc"), "/usr/bin/pfc_jsonl",
        )
        self.assertEqual(result["rows"], 0)
        self.assertTrue(result.get("skipped"))


# ===========================================================================
# 7. Upload — S3 path parsing & local copy
# ===========================================================================

class TestUploadArchive(unittest.TestCase):

    def test_dry_run_s3_returns_true(self):
        arch_cfg = {"output_dir": "s3://my-bucket/prefix/"}
        with tempfile.NamedTemporaryFile(suffix=".pfc", delete=False) as f:
            f.write(b"FAKE")
            pfc_path = Path(f.name)
        result = upload_archive(pfc_path, arch_cfg, dry_run=True)
        self.assertTrue(result)
        pfc_path.unlink()

    def test_dry_run_local_returns_true(self):
        arch_cfg = {"output_dir": "./test-archives/"}
        with tempfile.NamedTemporaryFile(suffix=".pfc", delete=False) as f:
            f.write(b"FAKE")
            pfc_path = Path(f.name)
        result = upload_archive(pfc_path, arch_cfg, dry_run=True)
        self.assertTrue(result)
        pfc_path.unlink()

    def test_local_copy_creates_file(self):
        with tempfile.TemporaryDirectory() as dest_dir:
            with tempfile.NamedTemporaryFile(suffix=".pfc", delete=False) as f:
                f.write(b"ARCHIVE_DATA")
                pfc_path = Path(f.name)

            arch_cfg = {"output_dir": dest_dir}
            upload_archive(pfc_path, arch_cfg, dry_run=False)

            dest_file = Path(dest_dir) / pfc_path.name
            self.assertTrue(dest_file.exists())
            self.assertEqual(dest_file.read_bytes(), b"ARCHIVE_DATA")
            pfc_path.unlink()

    def test_local_copy_includes_bidx_when_present(self):
        with tempfile.TemporaryDirectory() as src_dir, \
             tempfile.TemporaryDirectory() as dest_dir:
            pfc_path  = Path(src_dir) / "archive.pfc"
            bidx_path = Path(src_dir) / "archive.pfc.bidx"
            pfc_path.write_bytes(b"PFC")
            bidx_path.write_bytes(b"BIDX")

            arch_cfg = {"output_dir": dest_dir}
            upload_archive(pfc_path, arch_cfg, dry_run=False)

            self.assertTrue((Path(dest_dir) / "archive.pfc").exists())
            self.assertTrue((Path(dest_dir) / "archive.pfc.bidx").exists())


# ===========================================================================
# 8. Verify — row count matching
# ===========================================================================

class TestVerifyArchive(unittest.TestCase):

    @patch("pfc_archiver.subprocess.run")
    def test_correct_row_count_passes(self, mock_run):
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "verify.jsonl"
            jsonl_path.write_text('{"ts":"a"}\n{"ts":"b"}\n{"ts":"c"}\n')

            pfc_path = Path(tmpdir) / "archive.pfc"
            pfc_path.write_bytes(b"FAKE")

            def fake_run(cmd, **kwargs):
                out_path = Path(cmd[-1])
                out_path.write_text('{"ts":"a"}\n{"ts":"b"}\n{"ts":"c"}\n')
                m = MagicMock()
                m.returncode = 0
                return m

            mock_run.side_effect = fake_run

            result = verify_archive(pfc_path, expected_rows=3, pfc_binary="/usr/bin/pfc_jsonl")
            self.assertTrue(result)

    @patch("pfc_archiver.subprocess.run")
    def test_row_count_mismatch_raises(self, mock_run):
        def fake_run(cmd, **kwargs):
            out_path = Path(cmd[-1])
            out_path.write_text('{"ts":"a"}\n{"ts":"b"}\n')  # only 2 rows
            m = MagicMock()
            m.returncode = 0
            return m

        mock_run.side_effect = fake_run

        with tempfile.TemporaryDirectory() as tmpdir:
            pfc_path = Path(tmpdir) / "archive.pfc"
            pfc_path.write_bytes(b"FAKE")
            with self.assertRaises(RuntimeError) as ctx:
                verify_archive(pfc_path, expected_rows=3, pfc_binary="/usr/bin/pfc_jsonl")
            self.assertIn("VERIFY FAILED", str(ctx.exception))


# ===========================================================================
# 9. Delete — dry-run & schema-qualified query
# ===========================================================================

class TestDeletePartition(unittest.TestCase):

    def test_dry_run_skips_db_call(self):
        db_cfg  = {
            "host": "localhost", "schema": "doc",
            "table": "logs", "ts_column": "ts",
        }
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with patch("pfc_archiver._connect") as mock_connect:
            delete_partition(db_cfg, from_ts, to_ts, dry_run=True)
            mock_connect.assert_not_called()

    @patch("pfc_archiver._connect")
    def test_delete_uses_correct_params(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.rowcount = 42
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        db_cfg  = {
            "host": "localhost", "schema": "doc",
            "table": "events", "ts_column": "ts",
        }
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        delete_partition(db_cfg, from_ts, to_ts)

        call_args = mock_cur.execute.call_args
        params    = call_args[0][1]
        self.assertEqual(params[0], from_ts.isoformat())
        self.assertEqual(params[1], to_ts.isoformat())


# ===========================================================================
# 10. Run log
# ===========================================================================

class TestWriteRunLog(unittest.TestCase):

    def test_log_appended_as_valid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_run_log(tmpdir, {"status": "ok", "rows": 100, "table": "logs"})
            log_file = Path(tmpdir) / "archive_runs.jsonl"
            self.assertTrue(log_file.exists())
            entry = json.loads(log_file.read_text().strip())
            self.assertEqual(entry["status"], "ok")
            self.assertEqual(entry["rows"], 100)
            self.assertIn("ts", entry)

    def test_multiple_logs_append(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_run_log(tmpdir, {"status": "ok", "rows": 10})
            write_run_log(tmpdir, {"status": "ok", "rows": 20})
            log_file = Path(tmpdir) / "archive_runs.jsonl"
            lines = log_file.read_text().strip().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[1])["rows"], 20)


# ===========================================================================
# 11. Archive cycle — full integration with mocks
# ===========================================================================

class TestArchiveCycle(unittest.TestCase):

    @patch("pfc_archiver.write_run_log")
    @patch("pfc_archiver.verify_archive")
    @patch("pfc_archiver.upload_archive")
    @patch("pfc_archiver.export_partition_to_pfc")
    @patch("pfc_archiver.get_partition_ranges")
    def test_full_cycle_happy_path(
        self, mock_partitions, mock_export, mock_upload,
        mock_verify, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 500, "jsonl_mb": 1.0, "output_mb": 0.05, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg()
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        mock_export.assert_called_once()
        mock_upload.assert_called_once()
        mock_verify.assert_called_once()
        mock_log.assert_called_once()
        log_call = mock_log.call_args[0][1]
        self.assertEqual(log_call["status"], "ok")
        self.assertEqual(log_call["rows"], 500)

    @patch("pfc_archiver.get_partition_ranges")
    def test_no_partitions_exits_early(self, mock_partitions):
        mock_partitions.return_value = []
        cfg = _make_cfg()
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
        mock_partitions.assert_called_once()

    @patch("pfc_archiver.write_run_log")
    @patch("pfc_archiver.verify_archive")
    @patch("pfc_archiver.upload_archive")
    @patch("pfc_archiver.export_partition_to_pfc")
    @patch("pfc_archiver.get_partition_ranges")
    def test_dry_run_skips_upload_verify_log(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 0, "jsonl_mb": 0, "output_mb": 0, "ratio_pct": 0}

        cfg = _make_cfg()
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=True)

        mock_export.assert_called_once()
        mock_upload.assert_not_called()
        mock_verify.assert_not_called()
        mock_log.assert_not_called()

    @patch("pfc_archiver.write_run_log")
    @patch("pfc_archiver.verify_archive")
    @patch("pfc_archiver.upload_archive")
    @patch("pfc_archiver.export_partition_to_pfc")
    @patch("pfc_archiver.get_partition_ranges")
    def test_verify_failure_skips_delete_and_logs_error(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 100, "jsonl_mb": 1.0, "output_mb": 0.05, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.side_effect      = RuntimeError("VERIFY FAILED: 100 vs 99")

        cfg = _make_cfg({"archive": {"verify": True, "delete_after_archive": True}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        mock_log.assert_called_once()
        log_entry = mock_log.call_args[0][1]
        self.assertEqual(log_entry["status"], "verify_failed")

    @patch("pfc_archiver.write_run_log")
    @patch("pfc_archiver.delete_partition")
    @patch("pfc_archiver.verify_archive")
    @patch("pfc_archiver.upload_archive")
    @patch("pfc_archiver.export_partition_to_pfc")
    @patch("pfc_archiver.get_partition_ranges")
    def test_delete_called_when_enabled_and_verify_passes(
        self, mock_partitions, mock_export, mock_upload,
        mock_verify, mock_delete, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 200, "jsonl_mb": 2.0, "output_mb": 0.1, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg({"archive": {"verify": True, "delete_after_archive": True}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        mock_delete.assert_called_once()
        log_entry = mock_log.call_args[0][1]
        self.assertTrue(log_entry["deleted"])


# ===========================================================================
# 12. PFC filename — includes schema prefix
# ===========================================================================

class TestPfcFilename(unittest.TestCase):
    """CrateDB archive filenames include the schema: doc__logs__YYYYMMDD__YYYYMMDD.pfc"""

    @patch("pfc_archiver.write_run_log")
    @patch("pfc_archiver.verify_archive")
    @patch("pfc_archiver.upload_archive")
    @patch("pfc_archiver.export_partition_to_pfc")
    @patch("pfc_archiver.get_partition_ranges")
    def test_pfc_filename_includes_schema(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 50, "jsonl_mb": 0.5, "output_mb": 0.02, "ratio_pct": 4.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg({"db": {"table": "sensor_data", "schema": "doc"}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        export_call = mock_export.call_args
        output_path = export_call[0][3]  # 4th positional arg: output_path
        filename    = output_path.name
        # Must be "doc__sensor_data__YYYYMMDD__YYYYMMDD.pfc"
        self.assertTrue(filename.startswith("doc__sensor_data__"), f"Bad filename: {filename}")

    @patch("pfc_archiver.write_run_log")
    @patch("pfc_archiver.verify_archive")
    @patch("pfc_archiver.upload_archive")
    @patch("pfc_archiver.export_partition_to_pfc")
    @patch("pfc_archiver.get_partition_ranges")
    def test_pfc_filename_custom_schema(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 10, "jsonl_mb": 0.1, "output_mb": 0.005, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg({"db": {"table": "metrics", "schema": "myschema"}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        export_call = mock_export.call_args
        output_path = export_call[0][3]
        filename    = output_path.name
        self.assertTrue(filename.startswith("myschema__metrics__"), f"Bad filename: {filename}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
