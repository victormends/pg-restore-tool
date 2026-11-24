from profiles import profile_manager


def test_profile_crud_lifecycle(monkeypatch, tmp_path):
    profiles_file = tmp_path / "profiles.json"
    monkeypatch.setattr(profile_manager, "PROFILES_FILE", profiles_file)

    assert profile_manager.load_profiles() == []

    p1 = profile_manager.ConnectionProfile(
        name="local-dev", host="127.0.0.1", port="5432", user="postgres", saved_password=False
    )
    assert profile_manager.add_profile(p1) is True

    loaded = profile_manager.get_profile("local-dev")
    assert loaded is not None
    assert loaded.port == "5432"

    updated = profile_manager.ConnectionProfile(
        name="local-dev", host="127.0.0.1", port="5433", user="postgres", saved_password=True
    )
    assert profile_manager.add_profile(updated) is True
    all_profiles = profile_manager.load_profiles()
    assert len(all_profiles) == 1
    assert all_profiles[0].port == "5433"

    assert profile_manager.remove_profile("local-dev") is True
    assert profile_manager.load_profiles() == []


def test_match_pgpass_and_env_password(monkeypatch):
    cfg = {
        "host": "127.0.0.1",
        "port": "*",
        "database": "all",
        "username": "postgres",
        "password": "pgpass-value",
    }
    assert profile_manager.match_pgpass_entry(cfg, "127.0.0.1", "5432", "erp", "postgres") == "pgpass-value"
    assert profile_manager.match_pgpass_entry(cfg, "10.0.0.2", "5432", "erp", "postgres") is None
    assert profile_manager.match_pgpass_entry({}, "127.0.0.1", "5432", "erp", "postgres") is None

    monkeypatch.setenv("PGPASSWORD", "env-secret")
    assert profile_manager.get_env_password() == "env-secret"
