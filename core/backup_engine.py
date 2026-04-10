import os
import time
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Callable
from datetime import datetime

from core.restore_engine import _build_process_env


BACKUP_FORMATS = {
    'custom': '-Fc',
    'plain': '-Fp',
    'directory': '-Fd',
}


def _get_bin_path(pg_bin: str, tool: str) -> Path:
    is_windows = os.name == 'nt'
    ext = '.exe' if is_windows else ''
    return Path(pg_bin) / f'{tool}{ext}'


def pg_dump_available(pg_bin: str) -> bool:
    return _get_bin_path(pg_bin, 'pg_dump').exists()


def backup_database(pg_bin: str, host: str, port: str, user: str, password: str,
                    db_name: str, output_path: str, format: str = 'custom',
                    jobs: int = 1, process_env: Optional[dict[str, str]] = None,
                    progress_callback: Optional[Callable] = None) -> Tuple[bool, Optional[str], float]:
    env = process_env or _build_process_env(password)

    start_time = datetime.now()
    elapsed = 0.0
    
    try:
        pg_dump_path = _get_bin_path(pg_bin, 'pg_dump')
        
        if not pg_dump_path.exists():
            return False, f"pg_dump not found in {pg_bin}", 0.0
        
        format_flag = BACKUP_FORMATS.get(format, '-Fc')
        
        if progress_callback:
            progress_callback({'status': 'starting', 'message': f'Starting backup of {db_name}...'})
        
        if format == 'plain':
            result = subprocess.run(
                [str(pg_dump_path), '-U', user, '-h', host, '-p', port,
                 '-d', db_name, format_flag, '-f', output_path],
                capture_output=True,
                text=True,
                timeout=3600,
                env=env,
            )
        else:
            args = [str(pg_dump_path), '-U', user, '-h', host, '-p', port, '-d', db_name, format_flag]
            if format == 'directory':
                args.extend(['-j', str(jobs)])
            args.extend(['-f', output_path])
            result = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=3600,
                env=env,
            )
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if result.returncode == 0:
            if progress_callback:
                progress_callback({'status': 'complete', 'message': f'Backup completed ({elapsed:.1f}s)'})
            return True, None, elapsed
        else:
            if progress_callback:
                progress_callback({'status': 'error', 'message': result.stderr})
            return False, result.stderr, elapsed
    except subprocess.TimeoutExpired:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, "Backup timed out.", elapsed
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, str(e), elapsed


def generate_backup_filename(db_name: str) -> str:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{db_name}_backup_{timestamp}.dump"


def get_backup_size(output_path: str) -> float:
    try:
        return Path(output_path).stat().st_size / (1024 * 1024)
    except Exception:
        return 0.0


def verify_backup_file(output_path: str, pg_bin: str) -> Tuple[bool, Optional[str]]:
    try:
        pg_restore_path = _get_bin_path(pg_bin, 'pg_restore')
        
        if not pg_restore_path.exists():
            return False, f"pg_restore not found in {pg_bin}"
        
        result = subprocess.run(
            [str(pg_restore_path), '--list', output_path],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            return True, None
        else:
            return False, f"Invalid backup: {result.stderr}"
    except Exception as e:
        return False, str(e)


def get_backup_info(output_path: str, pg_bin: str) -> dict:
    import re
    version_pattern = re.compile(r'PostgreSQL (\d+\.\d+)')
    
    result = {'valid': False, 'db_name': None, 'version': None, 'size_mb': 0, 'objects': 0}
    
    valid, error = verify_backup_file(output_path, pg_bin)
    if not valid:
        result['error'] = error
        return result
    
    result['valid'] = True
    result['size_mb'] = get_backup_size(output_path)
    
    try:
        pg_restore_path = _get_bin_path(pg_bin, 'pg_restore')
        
        output = subprocess.run(
            [str(pg_restore_path), '--list', output_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if output.returncode == 0:
            for line in output.stdout.split('\n')[:30]:
                if 'Database:' in line:
                    parts = line.split('Database:')
                    if len(parts) > 1:
                        result['db_name'] = parts[1].strip().split()[0]
                if 'PostgreSQL' in line:
                    match = version_pattern.search(line)
                    if match:
                        result['version'] = match.group(1)
                
                if ';' in line:
                    result['objects'] += 1
    except Exception:
        pass
    
    return result


def cleanup_old_backups(backup_dir: str, keep_last: int = 5) -> int:
    try:
        path = Path(backup_dir)
        if not path.exists():
            return 0

        candidates = []
        for ext in ('*.dump', '*.backup', '*.sql', '*.bak'):
            candidates.extend(path.glob(ext))
        dumps = sorted(set(candidates), key=lambda p: p.stat().st_mtime, reverse=True)
        
        removed = 0
        for old_dump in dumps[keep_last:]:
            old_dump.unlink()
            removed += 1
        
        return removed
    except Exception:
        return 0


if __name__ == '__main__':
    print("Backup engine utilities")
