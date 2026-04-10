import os
import json
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass, asdict


def default_backup_dirs() -> List[str]:
    return [
        str(Path.home() / 'Desktop'),
        str(Path.home() / 'Downloads'),
        str(Path.home() / 'Documents'),
        str(Path.home() / 'Backups'),
        'C:/Backups',
    ]


@dataclass
class Config:
    backup_dirs: Optional[List[str]] = None
    default_jobs: int = 4
    turbo_mode: str = 'fast'
    maintenance_work_mem_mb: int = 512
    restore_factor: float = 3.0
    max_retries: int = 3
    retry_delay: float = 1.0
    log_file: Optional[str] = None
    skip_backup: bool = False
    
    def __post_init__(self):
        if self.backup_dirs is None:
            self.backup_dirs = default_backup_dirs()


DEFAULT_CONFIG = Config()


def get_config_path() -> Path:
    return Path.home() / '.pg_restore_config.json'


def load_config(config_path: Optional[str] = None) -> Config:
    if config_path:
        path = Path(config_path)
    else:
        path = get_config_path()
    
    if not path.exists():
        return DEFAULT_CONFIG
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return Config(**data)
    except Exception:
        return DEFAULT_CONFIG


def save_config(config: Config, config_path: Optional[str] = None) -> bool:
    if config_path:
        path = Path(config_path)
    else:
        path = get_config_path()
    
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(asdict(config), f, indent=2)
        return True
    except Exception:
        return False


def print_config(config: Config) -> str:
    backup_dirs = config.backup_dirs or []
    lines = [
        "═══════════════════════════════════════════",
        "            CONFIGURATION",
        "═══════════════════════════════════════════",
        f"Backup directories: {len(backup_dirs)}",
    ]
    for d in backup_dirs:
        lines.append(f"  - {d}")
    lines.extend([
        f"Default parallel jobs: {config.default_jobs}",
        f"Turbo mode: {config.turbo_mode}",
        f"Maintenance work mem: {config.maintenance_work_mem_mb} MB",
        f"Space factor: {config.restore_factor}",
        f"Max retries: {config.max_retries}",
        f"Retry delay: {config.retry_delay}s",
        f"Log file: {config.log_file or 'Default'}",
        f"Skip backup: {config.skip_backup}",
        "═══════════════════════════════════════════",
    ])
    return "\n".join(lines)


if __name__ == '__main__':
    config = load_config()
    print(print_config(config))
