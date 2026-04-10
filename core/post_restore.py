import os
import subprocess
from pathlib import Path
from typing import Optional, List, Tuple
import re

from core.restore_engine import _build_process_env


def _psql_path(pg_bin: str) -> str:
    return str(Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql'))


def run_analyze(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str,
                process_env: Optional[dict[str, str]] = None) -> Tuple[bool, Optional[str]]:
    env = process_env or _build_process_env(password)

    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', db_name, '-c', 'ANALYZE;'],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )

        return result.returncode == 0, None if result.returncode == 0 else result.stderr
    except Exception as e:
        return False, str(e)


def verify_tables_count(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str,
                        process_env: Optional[dict[str, str]] = None) -> int:
    env = process_env or _build_process_env(password)

    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', db_name, '-tAc',
             "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';"],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        if result.returncode == 0:
            return int(result.stdout.strip())
        return 0
    except Exception:
        return 0


def verify_views_count(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str,
                       process_env: Optional[dict[str, str]] = None) -> int:
    env = process_env or _build_process_env(password)

    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', db_name, '-tAc',
             "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='public';"],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        if result.returncode == 0:
            return int(result.stdout.strip())
        return 0
    except Exception:
        return 0


def verify_functions_count(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str,
                           process_env: Optional[dict[str, str]] = None) -> int:
    env = process_env or _build_process_env(password)

    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', db_name, '-tAc',
             "SELECT COUNT(*) FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');"],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        if result.returncode == 0:
            return int(result.stdout.strip())
        return 0
    except Exception:
        return 0


def validate_foreign_keys(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str,
                          process_env: Optional[dict[str, str]] = None) -> List[dict]:
    env = process_env or _build_process_env(password)
    violations = []

    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', db_name, '-tAc',
             """
             SELECT conrelid::regclass::text as table_name, conname as constraint_name
             FROM pg_constraint
             WHERE contype = 'f' AND NOT convalidated;
             """],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 2:
                        violations.append({
                            'table': parts[0].strip(),
                            'constraint': parts[1].strip()
                        })
        
        return violations
    except Exception:
        return violations


def sanity_check(pg_bin: str, host: str, port: str, user: str, password: str, db_name: str,
                 process_env: Optional[dict[str, str]] = None) -> Tuple[bool, Optional[str]]:
    env = process_env or _build_process_env(password)

    try:
        result = subprocess.run(
            [_psql_path(pg_bin), '-U', user, '-h', host, '-p', port, '-d', db_name, '-c', 'SELECT 1;'],
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
        )

        return result.returncode == 0, None if result.returncode == 0 else "Sanity check failed"
    except Exception as e:
        return False, str(e)


def log_restore_operation(log_path: str, backup_file: str, db_name: str, host: str, 
                          port: str, success: bool, duration: float, tables: int, 
                          views: int, functions: int, warnings: List[str]) -> None:
    from datetime import datetime
    
    timestamp = datetime.now().isoformat()
    status = "SUCCESS" if success else "FAILURE"
    
    lines = [
        f"[{timestamp}] {status}",
        f"  Backup: {backup_file}",
        f"  Target: {host}:{port}/{db_name}",
        f"  Duration: {duration:.1f}s",
        f"  Tables: {tables}",
        f"  Views: {views}",
        f"  Functions: {functions}",
    ]
    
    if warnings:
        lines.append(f"  Warnings: {len(warnings)}")
        for w in warnings:
            lines.append(f"    - {w}")
    
    lines.append("")
    
    try:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write('\n'.join(lines))
    except Exception:
        pass


if __name__ == '__main__':
    print("Post-restore validation utilities")
