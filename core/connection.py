import os
import re
import time
import subprocess
from pathlib import Path
from typing import Optional, Tuple
from dataclasses import dataclass


class ConnectionError(Exception):
    pass


@dataclass
class ConnectionResult:
    success: bool
    error: Optional[str] = None
    ssl_mode: Optional[str] = None
    is_superuser: bool = False
    can_create_db: bool = False
    retries: int = 0


def test_connection(pg_bin: str, host: str, port: str, user: str, password: str, 
                   ssl_mode: str = 'prefer', max_retries: int = 3, retry_delay: float = 1.0) -> ConnectionResult:
    
    last_result = ConnectionResult(success=False, error="No attempts made")
    
    for attempt in range(max_retries):
        result = _try_connection(pg_bin, host, port, user, password, ssl_mode)
        last_result = result
        
        if result.success:
            return result
        
        if attempt < max_retries - 1:
            time.sleep(retry_delay * (attempt + 1))
    
    return last_result


def _try_connection(pg_bin: str, host: str, port: str, user: str, password: str, ssl_mode: str) -> ConnectionResult:
    os.environ['PGPASSWORD'] = password
    os.environ['PGSSLMODE'] = ssl_mode
    
    try:
        psql_path = Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql')
        
        args = [str(psql_path), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc', 'SELECT 1;']
        
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0:
            is_super, can_create = _check_permissions(pg_bin, host, port, user, password, ssl_mode)
            return ConnectionResult(
                success=True,
                ssl_mode=ssl_mode,
                is_superuser=is_super,
                can_create_db=can_create
            )
        else:
            error_msg = result.stderr or result.stdout
            
            if 'authentication failed' in error_msg.lower():
                return ConnectionResult(success=False, error="Authentication failed. Check username and password.")
            elif 'could not connect' in error_msg.lower():
                return ConnectionResult(success=False, error="Could not connect to the server.")
            elif 'ssl' in error_msg.lower() and ssl_mode != 'disable':
                return test_connection(pg_bin, host, port, user, password, 'disable', max_retries=1)
            else:
                return ConnectionResult(success=False, error=error_msg.strip())
    except subprocess.TimeoutExpired:
        return ConnectionResult(success=False, error="Connection attempt timed out.")
    except Exception as e:
        return ConnectionResult(success=False, error=f"Error: {str(e)}")
    finally:
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']
        if 'PGSSLMODE' in os.environ:
            del os.environ['PGSSLMODE']


def _check_permissions(pg_bin: str, host: str, port: str, user: str, password: str, ssl_mode: str) -> Tuple[bool, bool]:
    os.environ['PGPASSWORD'] = password
    os.environ['PGSSLMODE'] = ssl_mode
    
    try:
        psql_path = Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql')
        
        result = subprocess.run(
            [str(psql_path), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc',
             "SELECT rolsuper, rolcreatedb FROM pg_roles WHERE rolname = current_user;"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            parts = result.stdout.strip().split('|')
            if len(parts) >= 2:
                is_super = parts[0].strip().lower() == 't'
                can_create = parts[1].strip().lower() == 't'
                return is_super, can_create
        
        return False, False
    except Exception:
        return False, False
    finally:
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']
        if 'PGSSLMODE' in os.environ:
            del os.environ['PGSSLMODE']


def check_hba_config_issue(pg_bin: str, host: str, port: str, user: str, password: str, ssl_mode: str = 'prefer') -> Optional[str]:
    os.environ['PGPASSWORD'] = password
    os.environ['PGSSLMODE'] = ssl_mode
    
    try:
        psql_path = Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql')
        
        result = subprocess.run(
            [str(psql_path), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc', 'SELECT 1;'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        stderr = result.stderr.lower()
        
        if 'no pg_hba.conf entry' in stderr:
            return "Add a pg_hba.conf entry for this user/host:\n  host all all 0.0.0.0/0 md5"
        elif 'authentication method' in stderr:
            return "The pg_hba.conf authentication method does not allow password auth. Use 'md5' or 'scram-sha-256'."
        
        return None
    except Exception:
        pass
    finally:
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']
        if 'PGSSLMODE' in os.environ:
            del os.environ['PGSSLMODE']
    
    return None


def detect_ssl_mode(pg_bin: str, host: str, port: str, user: str, password: str) -> str:
    ssl_modes = ['require', 'prefer', 'disable']
    
    for mode in ssl_modes:
        result = test_connection(pg_bin, host, port, user, password, ssl_mode=mode, max_retries=1)
        if result.success:
            return mode
    
    return 'disable'


def check_server_version(pg_bin: str, host: str, port: str, user: str, password: str) -> Optional[str]:
    os.environ['PGPASSWORD'] = password
    
    try:
        psql_path = Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql')
        
        result = subprocess.run(
            [str(psql_path), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc', 'SELECT version();'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            match = re.search(r'PostgreSQL (\d+\.\d+)', result.stdout)
            if match:
                return match.group(1)
    except Exception:
        pass
    finally:
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']
    
    return None


if __name__ == '__main__':
    print("Connection test utilities with retry and SSL support")
