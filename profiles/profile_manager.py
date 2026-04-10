import os
import re
import json
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass


@dataclass
class ConnectionProfile:
    name: str
    host: str
    port: str
    user: str
    saved_password: bool = False


PROFILES_FILE = Path.home() / '.pg_restore_profiles.json'


def load_profiles() -> List[ConnectionProfile]:
    if not PROFILES_FILE.exists():
        return []
    
    try:
        with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return [ConnectionProfile(**p) for p in data]
    except Exception:
        return []


def save_profiles(profiles: List[ConnectionProfile]) -> bool:
    try:
        with open(PROFILES_FILE, 'w', encoding='utf-8') as f:
            json.dump([p.__dict__ for p in profiles], f, indent=2)
        return True
    except Exception:
        return False


def add_profile(profile: ConnectionProfile) -> bool:
    profiles = load_profiles()
    
    existing = [i for i, p in enumerate(profiles) if p.name == profile.name]
    for i in reversed(existing):
        profiles.pop(i)
    
    profiles.append(profile)
    return save_profiles(profiles)


def remove_profile(name: str) -> bool:
    profiles = load_profiles()
    profiles = [p for p in profiles if p.name != name]
    return save_profiles(profiles)


def get_profile(name: str) -> Optional[ConnectionProfile]:
    profiles = load_profiles()
    for p in profiles:
        if p.name == name:
            return p
    return None


_keyring_available = None


def _check_keyring():
    global _keyring_available
    if _keyring_available is None:
        try:
            import keyring
            _keyring_available = True
        except ImportError:
            _keyring_available = False
    return _keyring_available


def save_password_to_keyring(profile_name: str, password: str) -> bool:
    if not _check_keyring():
        return False
    
    try:
        import keyring
        keyring.set_password("pg-restore-tool", profile_name, password)
        return True
    except Exception:
        return False


def get_password_from_keyring(profile_name: str) -> Optional[str]:
    if not _check_keyring():
        return None
    
    try:
        import keyring
        return keyring.get_password("pg-restore-tool", profile_name)
    except Exception:
        return None


def delete_password_from_keyring(profile_name: str) -> bool:
    if not _check_keyring():
        return False
    
    try:
        import keyring
        keyring.delete_password("pg-restore-tool", profile_name)
        return True
    except Exception:
        return False


def check_pgpass() -> Optional[dict]:
    pgpass_paths = [
        Path.home() / '.pgpass',
        Path(os.environ.get('PGPASSFILE', '')),
    ]
    
    for pgpass_path in pgpass_paths:
        if not pgpass_path or not pgpass_path.exists():
            continue
        
        try:
            with open(pgpass_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    parts = line.split(':')
                    if len(parts) >= 5:
                        return {
                            'host': parts[0],
                            'port': parts[1],
                            'database': parts[2],
                            'username': parts[3],
                            'password': parts[4]
                        }
        except Exception:
            pass
    
    return None


def match_pgpass_entry(pgpass_config: dict, host: str, port: str, database: str, user: str) -> Optional[str]:
    if not pgpass_config:
        return None
    
    host_match = pgpass_config['host'] in ('*', host)
    port_match = pgpass_config['port'] in ('*', port)
    db_match = pgpass_config['database'] in ('*', database, 'all')
    user_match = pgpass_config['username'] in ('*', user)
    
    if host_match and port_match and db_match and user_match:
        return pgpass_config['password']
    
    return None


def get_env_password() -> Optional[str]:
    return os.environ.get('PGPASSWORD')


if __name__ == '__main__':
    print("Profile management utilities")