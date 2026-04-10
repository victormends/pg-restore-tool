import os
import re
import subprocess
from pathlib import Path
from typing import Optional, List, Tuple
import sys


def _psql_path(pg_bin: str) -> str:
    return str(Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql'))


def sanitize_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    if '\x00' in identifier:
        raise ValueError("Identifier cannot contain null bytes")

    if len(identifier) > 63:
        raise ValueError("Identifier is too long for PostgreSQL")

    if re.search(r'[\r\n]', identifier):
        raise ValueError(f"Invalid identifier: {identifier}")

    if identifier.lower() in ('template0', 'template1'):
        raise ValueError(f"Reserved identifier: {identifier}")

    return identifier


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def list_databases(pg_bin: str, host: str, port: str, user: str, password: str) -> List[dict]:
    os.environ['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc',
             "SELECT datname, pg_database_size(datname) FROM pg_database WHERE datallowconn AND datname NOT LIKE 'template%' ORDER BY datname;"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            return []
        
        databases = []
        for line in result.stdout.strip().split('\n'):
            if '|' in line:
                parts = line.split('|')
                if len(parts) >= 2:
                    name = parts[0].strip()
                    try:
                        size_bytes = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
                    except (ValueError, AttributeError):
                        size_bytes = 0
                    databases.append({
                        'name': name,
                        'size_mb': round(size_bytes / (1024 * 1024), 1)
                    })
        
        return databases
    except Exception:
        return []
    finally:
        os.environ.pop('PGPASSWORD', None)


def database_exists(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str) -> bool:
    sanitized = sanitize_identifier(db_name)
    os.environ['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc',
             f"SELECT 1 FROM pg_database WHERE datname = {quote_literal(sanitized)};"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        return result.returncode == 0 and result.stdout.strip() == '1'
    except Exception:
        return False
    finally:
        os.environ.pop('PGPASSWORD', None)


def create_database(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str, encoding: str = 'UTF8') -> Tuple[bool, Optional[str]]:
    sanitized_db = sanitize_identifier(db_name)
    sanitized_user = sanitize_identifier(user)
    sanitized_encoding = re.sub(r'[^A-Za-z0-9_]', '', encoding)
    
    os.environ['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-v', 'ON_ERROR_STOP=1', '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-c',
             f'CREATE DATABASE {quote_identifier(sanitized_db)} OWNER {quote_identifier(sanitized_user)} ENCODING {sanitized_encoding};'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            return True, None
        else:
            return False, result.stderr or result.stdout
    except Exception as e:
        return False, str(e)
    finally:
        os.environ.pop('PGPASSWORD', None)


def drop_database(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str, force: bool = False) -> Tuple[bool, Optional[str]]:
    sanitized = sanitize_identifier(db_name)
    
    os.environ['PGPASSWORD'] = password
    
    try:
        if force:
            subprocess.run(
                [_psql_path(pg_bin), '-v', 'ON_ERROR_STOP=1', '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-c',
                 f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = {quote_literal(sanitized)} AND pid <> pg_backend_pid();"],
                capture_output=True,
                timeout=10
            )
        
        result = subprocess.run(
            [_psql_path(pg_bin), '-v', 'ON_ERROR_STOP=1', '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-c',
             f'DROP DATABASE IF EXISTS {quote_identifier(sanitized)};'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            return True, None
        else:
            return False, result.stderr or result.stdout
    except Exception as e:
        return False, str(e)
    finally:
        os.environ.pop('PGPASSWORD', None)


def get_data_directory(pg_bin: str, host: str, port: str, user: str, password: str) -> Optional[str]:
    os.environ['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc',
             'SHOW data_directory;'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except Exception:
        return None
    finally:
        os.environ.pop('PGPASSWORD', None)


def get_disk_space(path: str) -> Tuple[int, int]:
    import platform
    system = platform.system()
    
    if system == 'Windows':
        try:
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            total_bytes = ctypes.c_ulonglong(0)
            
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(path), None, ctypes.byref(total_bytes), ctypes.byref(free_bytes))
            
            return free_bytes.value, total_bytes.value
        except Exception:
            return 0, 0
    else:
        try:
            import shutil
            stat = shutil.disk_usage(path)
            return stat.free, stat.total
        except Exception:
            return 0, 0


def estimate_required_space(backup_size_mb: float, current_db_size_mb: float = 0, factor: float = 3.0) -> float:
    return (backup_size_mb * factor) - current_db_size_mb


def check_space_for_restore(pg_bin: str, host: str, port: str, user: str, password: str, 
                           backup_size_mb: float, target_db: str, drop_existing: bool = False) -> Tuple[bool, Optional[str], dict]:
    data_dir = get_data_directory(pg_bin, host, port, user, password)
    
    if not data_dir:
        return True, None, {'warning': 'Could not determine data directory'}
    
    free_bytes, total_bytes = get_disk_space(data_dir)
    free_mb = free_bytes / (1024 * 1024)
    total_gb = total_bytes / (1024 * 1024 * 1024)
    
    current_db_size_mb = 0
    if drop_existing and target_db:
        current_db_size_mb = get_database_size(pg_bin, host, port, user, password, target_db)
    
    required_mb = estimate_required_space(backup_size_mb, current_db_size_mb)
    
    if free_mb < required_mb:
        return False, f"Insufficient space: {free_mb:.1f} MB free, {required_mb:.1f} MB required", {
            'free_mb': free_mb,
            'required_mb': required_mb,
            'total_gb': round(total_gb, 1),
            'backup_size_mb': backup_size_mb
        }
    
    warning = None
    if free_mb < required_mb * 1.2:
        warning = f"Tight space: {free_mb:.1f} MB free for {required_mb:.1f} MB required"
    
    return True, warning, {
        'free_mb': free_mb,
        'required_mb': required_mb,
        'total_gb': round(total_gb, 1),
        'backup_size_mb': backup_size_mb,
        'current_db_size_mb': current_db_size_mb
    }


def get_database_size(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str) -> float:
    sanitized = sanitize_identifier(db_name)
    os.environ['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc',
             f"SELECT pg_database_size({quote_literal(sanitized)});"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and result.stdout.strip().isdigit():
            return int(result.stdout.strip()) / (1024 * 1024)
        return 0
    except Exception:
        return 0
    finally:
        os.environ.pop('PGPASSWORD', None)


def check_server_recovery_mode(pg_bin: str, host: str, port: str, user: str, password: str) -> Tuple[bool, Optional[str]]:
    os.environ['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc',
             'SELECT pg_is_in_recovery();'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            is_recovery = result.stdout.strip().lower() == 'true'
            if is_recovery:
                return True, "Server is in recovery mode (standby)."
        return False, None
    except Exception:
        return False, None
    finally:
        os.environ.pop('PGPASSWORD', None)


if __name__ == '__main__':
    print("Database operations utilities")
