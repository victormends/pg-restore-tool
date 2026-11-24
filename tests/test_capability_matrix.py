from types import SimpleNamespace

from core.capability_matrix import build_capability_matrix, print_capability_report


def test_capability_matrix_windows_binaries():
    env = SimpleNamespace(
        pg_installations=[r"C:\Program Files\PostgreSQL\16\bin"],
        pg_tools_available={
            r"C:\Program Files\PostgreSQL\16\bin": {
                "pg_restore.exe": True,
                "psql.exe": True,
                "pg_dump.exe": True,
            }
        },
    )
    cap = build_capability_matrix(env)
    assert cap.can_restore_custom is True
    assert cap.can_restore_tar is True
    assert cap.can_restore_dir is True
    assert cap.can_restore_sql is True
    assert cap.can_backup_before_restore is True
    assert cap.can_parallel_restore is True
    assert cap.max_parallel_jobs == 8
    assert cap.supported_pg_versions == ["16"]


def test_capability_matrix_linux_binaries():
    env = SimpleNamespace(
        pg_installations=["/usr/lib/postgresql/15/bin"],
        pg_tools_available={
            "/usr/lib/postgresql/15/bin": {
                "pg_restore": True,
                "psql": True,
                "pg_dump": False,
            }
        },
    )
    cap = build_capability_matrix(env)
    assert cap.can_restore_custom is True
    assert cap.can_restore_sql is True
    assert cap.can_backup_before_restore is False
    assert cap.can_parallel_restore is True
    assert cap.supported_pg_versions == ["15"]

    report = print_capability_report(cap)
    assert "Restore (custom format):  OK" in report
    assert "Pre-restore backup:        NO" in report
    assert "15" in report
