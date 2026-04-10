from dataclasses import dataclass
from typing import Optional


@dataclass
class CapabilityMatrix:
    can_restore_custom: bool
    can_restore_tar: bool
    can_restore_dir: bool
    can_restore_sql: bool
    can_backup_before_restore: bool
    can_parallel_restore: bool
    can_use_keyring: bool
    max_parallel_jobs: int
    supported_pg_versions: list[str]


class EnvCheckResult:
    pass


def build_capability_matrix(env_result) -> CapabilityMatrix:
    can_restore_custom = False
    can_restore_tar = False
    can_restore_dir = False
    can_restore_sql = False
    can_backup_before_restore = False
    can_parallel_restore = False
    max_parallel_jobs = 1
    
    pg_tools = getattr(env_result, 'pg_tools_available', {})
    for pg_bin, tools in pg_tools.items():
        if isinstance(tools, dict):
            if tools.get('pg_restore.exe', False):
                can_restore_custom = True
                can_restore_tar = True
                can_restore_dir = True
                can_parallel_restore = True
                max_parallel_jobs = 8
            
            if tools.get('psql.exe', False):
                can_restore_sql = True
                
            if tools.get('pg_dump.exe', False):
                can_backup_before_restore = True
    
    can_use_keyring = False
    try:
        import keyring
        can_use_keyring = True
    except ImportError:
        pass
    
    versions = []
    for pg_path in env_result.pg_installations:
        try:
            import re
            match = re.search(r'PostgreSQL[\\/](\d+)', pg_path)
            if match:
                versions.append(match.group(1))
        except Exception:
            pass
    
    return CapabilityMatrix(
        can_restore_custom=can_restore_custom,
        can_restore_tar=can_restore_tar,
        can_restore_dir=can_restore_dir,
        can_restore_sql=can_restore_sql,
        can_backup_before_restore=can_backup_before_restore,
        can_parallel_restore=can_parallel_restore,
        can_use_keyring=can_use_keyring,
        max_parallel_jobs=max_parallel_jobs,
        supported_pg_versions=versions
    )


def print_capability_report(cap: CapabilityMatrix) -> str:
    lines = [
        "═══════════════════════════════════════════",
        "        CAPABILITY MATRIX",
        "═══════════════════════════════════════════",
        f"Restore (custom format):  {'OK' if cap.can_restore_custom else 'NO'}",
        f"Restore (tar format):      {'OK' if cap.can_restore_tar else 'NO'}",
        f"Restore (directory):       {'OK' if cap.can_restore_dir else 'NO'}",
        f"Restore (SQL plain):       {'OK' if cap.can_restore_sql else 'NO'}",
        f"Pre-restore backup:        {'OK' if cap.can_backup_before_restore else 'NO'}",
        f"Parallel restore:          {'OK' if cap.can_parallel_restore else 'NO'}",
        f"Max parallel jobs:         {cap.max_parallel_jobs}",
        f"Keyring (passwords):       {'OK' if cap.can_use_keyring else 'NO'}",
        f"Supported PG versions:     {', '.join(cap.supported_pg_versions) or 'None'}",
        "═══════════════════════════════════════════",
    ]
    return "\n".join(lines)


if __name__ == '__main__':
    from core.env_check import run_env_check
    env = run_env_check()
    cap = build_capability_matrix(env)
    print(print_capability_report(cap))
