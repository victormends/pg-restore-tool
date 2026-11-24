from types import SimpleNamespace
from typing import Any, List

from core import restore_engine


def test_pgoptions_for_profile_bounds_and_modes():
    assert restore_engine._pgoptions_for_profile("safe", 512) is None
    assert restore_engine._pgoptions_for_profile("unsafe", 512) is None

    fast_low = restore_engine._pgoptions_for_profile("fast", 8)
    assert fast_low is not None
    assert "synchronous_commit=off" in fast_low
    assert "maintenance_work_mem=64MB" in fast_low

    fast_high = restore_engine._pgoptions_for_profile("fast", 99999)
    assert fast_high is not None
    assert "maintenance_work_mem=8192MB" in fast_high

    fast_invalid = restore_engine._pgoptions_for_profile("fast", "not-an-int")  # type: ignore[arg-type]
    assert fast_invalid is not None
    assert "maintenance_work_mem=512MB" in fast_invalid


def test_enable_turbo_mode_safe_fast_and_invalid():
    ok, err, ctx = restore_engine.enable_turbo_mode(
        pg_bin="/usr/bin",
        host="127.0.0.1",
        port="5432",
        user="postgres",
        password="secret-value",
        profile="invalid_profile",
    )
    assert ok is False
    assert err is not None and "Unknown turbo mode profile" in err
    assert ctx.profile == "safe"

    ok_safe, err_safe, ctx_safe = restore_engine.enable_turbo_mode(
        pg_bin="/usr/bin",
        host="127.0.0.1",
        port="5432",
        user="postgres",
        password="secret-value",
        profile="safe",
    )
    assert ok_safe is True
    assert err_safe is None
    assert ctx_safe.env["PGPASSWORD"] == "secret-value"
    assert "PGOPTIONS" not in ctx_safe.env or ctx_safe.env.get("PGOPTIONS") != restore_engine._pgoptions_for_profile("fast", 512)

    ok_fast, err_fast, ctx_fast = restore_engine.enable_turbo_mode(
        pg_bin="/usr/bin",
        host="127.0.0.1",
        port="5432",
        user="postgres",
        password="secret-value",
        profile="fast",
        maintenance_work_mem_mb=256,
    )
    assert ok_fast is True
    assert err_fast is None
    assert ctx_fast.env["PGPASSWORD"] == "secret-value"
    assert "maintenance_work_mem=256MB" in ctx_fast.env["PGOPTIONS"]


def test_unsafe_profile_captures_and_restores_previous_settings(monkeypatch):
    calls: List[List[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if "-tAc" in cmd:
            return SimpleNamespace(
                returncode=0,
                stdout="fsync|on\nfull_page_writes|on\nsynchronous_commit|on\n",
                stderr="",
            )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(restore_engine.subprocess, "run", fake_run)

    ok, err, ctx = restore_engine.enable_turbo_mode(
        pg_bin="/usr/lib/postgresql/16/bin",
        host="127.0.0.1",
        port="5432",
        user="postgres",
        password="pw",
        profile="unsafe",
    )
    assert ok is True
    assert err is None
    assert ctx.previous_settings == {
        "fsync": "on",
        "full_page_writes": "on",
        "synchronous_commit": "on",
    }
    assert any("ALTER SYSTEM SET fsync = off" in " ".join(c) for c in calls)

    ok_dis, err_dis = restore_engine.disable_turbo_mode(
        pg_bin="/usr/lib/postgresql/16/bin",
        host="127.0.0.1",
        port="5432",
        user="postgres",
        password="pw",
        ctx=ctx,
    )
    assert ok_dis is True
    assert err_dis is None
    last_cmd = " ".join(calls[-1])
    assert "ALTER SYSTEM SET fsync = 'on'" in last_cmd
    assert "SELECT pg_reload_conf();" in last_cmd


def test_disable_turbo_mode_noop_for_safe_and_fast():
    for profile in ("safe", "fast"):
        ctx = restore_engine.TurboModeContext(profile=profile, env={"PGPASSWORD": "pw"})
        ok, err = restore_engine.disable_turbo_mode("/usr/bin", "127.0.0.1", "5432", "postgres", "pw", ctx)
        assert ok is True
        assert err is None


def test_estimate_size_and_restore_progress():
    assert restore_engine.estimate_restore_size(10.0) == 30.0
    assert restore_engine.estimate_restore_size(10.0, factor=2.5) == 25.0

    prog = restore_engine.RestoreProgress()
    prog.start()
    prog.update("public.orders")
    prog.update("public.invoices")
    stats: dict[str, Any] = prog.get_stats()
    assert stats["objects_processed"] == 2
    assert stats["current_object"] == "public.invoices"
    assert stats["is_running"] is True
    prog.stop()
    assert prog.get_stats()["is_running"] is False
