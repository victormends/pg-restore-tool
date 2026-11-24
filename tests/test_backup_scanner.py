import os
from pathlib import Path
from types import SimpleNamespace

from core import backup_scanner


def test_detect_backup_type_variants(tmp_path):
    sql_path = tmp_path / "dump.sql"
    sql_path.write_text("-- SQL", encoding="utf-8")
    assert backup_scanner.detect_backup_type(sql_path) == "sql"

    custom_path = tmp_path / "app.backup"
    custom_path.write_bytes(b"PGDMP1234")
    assert backup_scanner.detect_backup_type(custom_path) == "custom"

    empty_dump = tmp_path / "empty.dump"
    empty_dump.write_bytes(b"")
    assert backup_scanner.detect_backup_type(empty_dump) == "unknown"

    bak_path = tmp_path / "app.bak"
    bak_path.write_bytes(b"PGDMP")
    assert backup_scanner.detect_backup_type(bak_path) == "custom"

    toc_dir = tmp_path / "dir_backup"
    toc_dir.mkdir()
    (toc_dir / "toc.dat").write_bytes(b"TOC")
    marker = toc_dir / "meta.dump"
    marker.write_bytes(b"DATA")
    assert backup_scanner.detect_backup_type(marker) == "directory"


def test_is_suspicious_empty_and_tmp_sibling(tmp_path):
    empty_file = tmp_path / "zero.dump"
    empty_file.write_bytes(b"")
    susp_empty, reason_empty = backup_scanner.is_suspicious(empty_file)
    assert susp_empty is True
    assert reason_empty is not None and "Empty" in reason_empty

    in_progress = tmp_path / "db.backup"
    in_progress.write_bytes(b"PGDMP")
    (tmp_path / "db.tmp").write_bytes(b"partial")
    susp_tmp, reason_tmp = backup_scanner.is_suspicious(in_progress)
    assert susp_tmp is True
    assert reason_tmp is not None and "Temporary" in reason_tmp

    healthy = tmp_path / "healthy.dump"
    healthy.write_bytes(b"PGDMP-valid")
    susp_ok, reason_ok = backup_scanner.is_suspicious(healthy)
    assert susp_ok is False
    assert reason_ok is None


def test_scan_backup_dirs_sorted_and_metadata(monkeypatch, tmp_path):
    older = tmp_path / "older.dump"
    newer = tmp_path / "newer.sql"
    ignored = tmp_path / "notes.txt"

    older.write_bytes(b"PGDMP" * 10)
    newer.write_text("SELECT 1;", encoding="utf-8")
    ignored.write_text("ignore", encoding="utf-8")

    os.utime(older, (1_700_000_000, 1_700_000_000))
    os.utime(newer, (1_700_001_000, 1_700_001_000))

    def fake_run(cmd, **kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout="; Archive created\n; Database: synthetic_erp\n; PostgreSQL version: 16.2\n",
            stderr="",
        )

    monkeypatch.setattr(backup_scanner.subprocess, "run", fake_run)

    found = backup_scanner.scan_backup_dirs(dirs=[tmp_path], pg_bin="/usr/bin")
    assert [item.name for item in found] == ["newer.sql", "older.dump"]
    assert found[1].source_db == "synthetic_erp"
    assert found[1].pg_version is not None and "16.2" in found[1].pg_version
