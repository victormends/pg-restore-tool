import os
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass
import subprocess
from datetime import datetime

from core.backup_engine import _get_bin_path
from core.config import default_backup_dirs


@dataclass
class BackupFile:
    path: str
    name: str
    size_mb: float
    extension: str
    modified: float
    backup_type: str
    source_db: Optional[str] = None
    pg_version: Optional[str] = None
    encoding: Optional[str] = None
    object_count: Optional[int] = None
    is_suspicious: bool = False
    suspicious_reason: Optional[str] = None


BACKUP_EXTENSIONS = {'.backup', '.dump', '.sql', '.bak'}

DEFAULT_SCAN_DIRS = [
    Path(p) for p in default_backup_dirs()
]


def detect_backup_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == '.sql':
        return 'sql'
    elif ext in ('.backup', '.dump'):
        parent_dir = path.parent
        if (parent_dir / 'toc.dat').exists():
            return 'directory'
        elif ext == '.dump' and not (path.stat().st_size > 0):
            return 'unknown'
        else:
            return 'custom'
    elif ext == '.bak':
        return 'custom'
    return 'unknown'


def is_suspicious(path: Path) -> tuple[bool, Optional[str]]:
    size = path.stat().st_size
    
    if size == 0:
        return True, "Empty file (possible placeholder)"
    
    if (path.parent / f"{path.stem}.tmp").exists():
        return True, "Temporary file detected"
    
    import ctypes
    try:
        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
        if attrs & 0x400:
            return True, "OneDrive temporary file"
    except Exception:
        pass
    
    return False, None


def extract_backup_metadata(path: Path, pg_bin: str) -> Optional[dict]:
    try:
        result = subprocess.run(
            [str(_get_bin_path(pg_bin, 'pg_restore')), '--list', str(path)],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            return None
        
        metadata = {}
        output = result.stdout
        
        for line in output.split('\n')[:20]:
            if 'Database:' in line:
                metadata['source_db'] = line.split('Database:')[1].strip().split()[0]
            if 'PostgreSQL' in line:
                metadata['pg_version'] = line.strip()
        
        return metadata
    except Exception:
        return None


def scan_backup_dirs(dirs: Optional[List[Path]] = None, pg_bin: Optional[str] = None) -> List[BackupFile]:
    if dirs is None:
        dirs = DEFAULT_SCAN_DIRS
    
    backups = []
    
    for scan_dir in dirs:
        if not scan_dir.exists():
            continue
            
        try:
            for item in scan_dir.iterdir():
                if item.is_file() and item.suffix.lower() in BACKUP_EXTENSIONS:
                    
                    is_susp, reason = is_suspicious(item)
                    backup_type = detect_backup_type(item)
                    
                    bf = BackupFile(
                        path=str(item),
                        name=item.name,
                        size_mb=round(item.stat().st_size / (1024 * 1024), 1),
                        extension=item.suffix.lower(),
                        modified=item.stat().st_mtime,
                        backup_type=backup_type,
                        is_suspicious=is_susp,
                        suspicious_reason=reason
                    )
                    
                    if pg_bin and backup_type in ('custom', 'directory'):
                        meta = extract_backup_metadata(item, pg_bin)
                        if meta:
                            bf.source_db = meta.get('source_db')
                            bf.pg_version = meta.get('pg_version')
                    
                    backups.append(bf)
        except PermissionError:
            continue
    
    backups.sort(key=lambda x: x.modified, reverse=True)
    return backups


def format_backup_list(backups: List[BackupFile]) -> str:
    lines = [
        "═══════════════════════════════════════════════════════",
        "              BACKUP FILES FOUND",
        "═══════════════════════════════════════════════════════",
    ]
    
    for i, b in enumerate(backups, 1):
        date_str = datetime.fromtimestamp(b.modified).strftime('%Y-%m-%d %H:%M')
        size_str = f"{b.size_mb:.1f} MB"
        
        status = ""
        if b.is_suspicious:
            status = " [WARNING]"
        
        lines.append(f"  [{i:2}] {b.name}")
        lines.append(f"       Type: {b.backup_type:10} | Size: {size_str:>10} | Date: {date_str}")
        
        if b.source_db:
            lines.append(f"       Source DB: {b.source_db}")
        if b.pg_version:
            lines.append(f"       PG: {b.pg_version}")
        if b.is_suspicious and b.suspicious_reason:
            lines.append(f"       WARNING: {b.suspicious_reason}")
        
        lines.append("")
    
    return "\n".join(lines)


if __name__ == '__main__':
    backups = scan_backup_dirs()
    print(format_backup_list(backups))
