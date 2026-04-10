import os
import sys
import platform
import subprocess
import re
import shutil
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from core.config import default_backup_dirs


@dataclass
class EnvCheckResult:
    os_ok: bool
    os_version: str
    terminal_utf8: bool
    pg_installations: list[str]
    pg_tools_available: dict[str, bool]
    pg_services: list[dict]
    backup_dirs_writable: bool
    temp_writable: bool
    warnings: list[str]


def check_os_and_terminal() -> tuple[str, bool]:
    system = platform.system()
    os_version = platform.version()
    utf8_ok = sys.getdefaultencoding().lower() == 'utf-8' or os.environ.get('PYTHONIOENCODING', '').startswith('utf')
    return f"{system} {os_version}", utf8_ok


def find_postgresql_installations() -> list[str]:
    installations = set()
    
    base_paths = [
        Path("C:/Program Files/PostgreSQL"),
        Path("C:/PostgreSQL"),
        Path("D:/PostgreSQL"),
        Path("E:/PostgreSQL"),
    ]
    
    for base in base_paths:
        if base.exists():
            for item in base.iterdir():
                if item.is_dir() and item.name[0].isdigit():
                    bin_path = item / "bin"
                    if (bin_path / "psql.exe").exists():
                        installations.add(str(bin_path))

    psql_on_path = shutil.which('psql.exe' if os.name == 'nt' else 'psql')
    if psql_on_path:
        installations.add(str(Path(psql_on_path).parent))

    return sorted(installations, reverse=True)


def check_pg_tools(pg_bin: str) -> dict[str, bool]:
    tools = {}
    for tool in ['psql.exe', 'pg_restore.exe', 'pg_dump.exe']:
        tools[tool] = (Path(pg_bin) / tool).exists()
    return tools


def detect_pg_services() -> list[dict]:
    services = []
    try:
        result = subprocess.run(
            [
                'powershell',
                '-NoProfile',
                '-Command',
                "Get-CimInstance Win32_Service | Where-Object { $_.Name -like '*postgre*' -or $_.DisplayName -like '*Postgre*' } | Select-Object Name, State, PathName"
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode != 0:
            return services

        current = None
        for raw_line in result.stdout.splitlines():
            line = raw_line.strip()
            if not line or line.startswith('Name') or line.startswith('----'):
                continue

            if current is None:
                parts = line.split(None, 2)
                if len(parts) >= 2:
                    current = {
                        'service_name': parts[0],
                        'status': parts[1].lower(),
                        'port': '5432',
                    }
                    if len(parts) == 3:
                        port_match = re.search(r'-p\s+(\d+)', parts[2])
                        if port_match:
                            current['port'] = port_match.group(1)
                    services.append(current)
                    current = None
    except Exception:
        pass
    return services


def check_permissions() -> tuple[bool, bool]:
    temp_dir = Path(os.environ.get('TEMP', 'C:/Temp'))
    backup_candidates = [Path(p) for p in default_backup_dirs()]
    
    temp_ok = temp_dir.exists() and os.access(temp_dir, os.W_OK)
    backup_dirs_ok = any(d.exists() and os.access(d, os.R_OK) for d in backup_candidates)
    
    return backup_dirs_ok, temp_ok


def run_env_check() -> EnvCheckResult:
    os_version, utf8_ok = check_os_and_terminal()
    pg_installations = find_postgresql_installations()
    
    pg_tools = {}
    for pg_bin in pg_installations:
        pg_tools[pg_bin] = check_pg_tools(pg_bin)
    
    services = detect_pg_services()
    backup_dirs_ok, temp_ok = check_permissions()
    
    warnings = []
    if not pg_installations:
        warnings.append("No PostgreSQL installation found")
    if not any(t.get('pg_restore.exe', False) for t in pg_tools.values()):
        warnings.append("pg_restore.exe not found - restore unavailable")
    if not temp_ok:
        warnings.append("Temporary directory is not writable")
    
    return EnvCheckResult(
        os_ok=True,
        os_version=os_version,
        terminal_utf8=utf8_ok,
        pg_installations=pg_installations,
        pg_tools_available=pg_tools,
        pg_services=services,
        backup_dirs_writable=backup_dirs_ok,
        temp_writable=temp_ok,
        warnings=warnings
    )


if __name__ == '__main__':
    result = run_env_check()
    print(f"OS: {result.os_version}")
    print(f"UTF8 Terminal: {result.terminal_utf8}")
    print(f"PG Installations: {result.pg_installations}")
    print(f"Warnings: {result.warnings}")
