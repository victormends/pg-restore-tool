import subprocess
import re
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class PgInstance:
    host: str
    port: str
    version: Optional[str]
    service_name: Optional[str]
    status: str
    pg_bin_path: Optional[str]


def detect_pg_instances() -> list[PgInstance]:
    instances = []
    
    try:
        result = subprocess.run(
            ['sc', 'query', 'postgresql-x64-1'],
            capture_output=True,
            text=True
        )
        
        if 'RUNNING' in result.stdout:
            port_match = re.search(r'-p\s+(\d+)', result.stdout)
            port = port_match.group(1) if port_match else '5432'
            
            svc_name_match = re.search(r'SERVICE_NAME:\s*(\S+)', result.stdout)
            service_name = svc_name_match.group(1) if svc_name_match else None
            
            pg_bin = find_pg_bin_for_port(port)
            
            instances.append(PgInstance(
                host='127.0.0.1',
                port=port,
                version=None,
                service_name=service_name,
                status='running',
                pg_bin_path=pg_bin
            ))
    except Exception:
        pass
    
    if not instances:
        instances.extend(scan_tcp_ports())
    
    return instances


def find_pg_bin_for_port(port: str) -> Optional[str]:
    base_paths = [
        Path("C:/Program Files/PostgreSQL"),
        Path("C:/PostgreSQL"),
    ]
    
    for base in base_paths:
        if base.exists():
            for item in base.iterdir():
                if item.is_dir() and item.name[0].isdigit():
                    if (item / "bin" / "psql.exe").exists():
                        return str(item / "bin")
    
    return None


def scan_tcp_ports() -> list[PgInstance]:
    instances = []
    try:
        result = subprocess.run(
            ['netstat', '-ano'],
            capture_output=True,
            text=True
        )
        
        ports = set()
        for line in result.stdout.split('\n'):
            if 'LISTENING' in line and 'TCP' in line:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        local_addr = parts[1]
                        if ':' in local_addr:
                            port = int(local_addr.split(':')[-1])
                            if 5400 <= port <= 5500:
                                ports.add(port)
                    except (ValueError, IndexError):
                        continue
        
        for port in sorted(ports):
            pg_bin = find_pg_bin_for_port(str(port))
            instances.append(PgInstance(
                host='127.0.0.1',
                port=str(port),
                version=None,
                service_name=f"(TCP direto)",
                status='listen',
                pg_bin_path=pg_bin
            ))
    except Exception:
        pass
    
    return instances


def get_pg_version(pg_bin: str, host: str, port: str, user: str, password: str) -> Optional[str]:
    import os
    os.environ['PGPASSWORD'] = password
    
    try:
        result = subprocess.run(
            [str(Path(pg_bin) / 'psql.exe'), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc', 'SELECT version();'],
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
        os.environ.pop('PGPASSWORD', None)
    
    return None


def format_instance_list(instances: list[PgInstance]) -> str:
    if not instances:
        return "No PostgreSQL instances found."
    
    lines = [
        "═══════════════════════════════════════════",
        "      ACTIVE POSTGRESQL INSTANCES",
        "═══════════════════════════════════════════",
    ]
    
    for i, inst in enumerate(instances, 1):
        status_icon = "●" if inst.status == 'running' else "○"
        version_str = f"v{inst.version}" if inst.version else "(version not detected)"
        lines.append(f"  [{i}] {inst.host}:{inst.port} {version_str}")
        lines.append(f"      Service: {inst.service_name} {status_icon}")
        lines.append("")
    
    return "\n".join(lines)


if __name__ == '__main__':
    instances = detect_pg_instances()
    print(format_instance_list(instances))
