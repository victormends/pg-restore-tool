import os
import re
import time
import threading
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable, Tuple
from datetime import datetime


class RestoreProgress:
    def __init__(self):
        self.current_object = None
        self.objects_processed = 0
        self.total_objects = 0
        self.start_time = None
        self.last_update = None
        self.is_running = False
        self._lock = threading.Lock()
    
    def start(self):
        self.start_time = datetime.now()
        self.last_update = self.start_time
        self.is_running = True
        self.objects_processed = 0
        self.current_object = None
    
    def update(self, object_name: Optional[str] = None):
        with self._lock:
            if object_name is not None:
                self.current_object = object_name
            self.objects_processed += 1
            self.last_update = datetime.now()
    
    def stop(self):
        self.is_running = False
    
    def get_stats(self) -> dict:
        with self._lock:
            now = datetime.now()
            elapsed = (now - self.start_time).total_seconds() if self.start_time else 0
            return {
                'objects_processed': self.objects_processed,
                'current_object': self.current_object,
                'elapsed_seconds': elapsed,
                'is_running': self.is_running,
            }


def estimate_restore_size(backup_size_mb: float, factor: float = 3.0) -> float:
    return backup_size_mb * factor


@dataclass
class TurboModeContext:
    profile: str
    env: dict[str, str]
    previous_settings: Optional[dict[str, str]] = None


def _build_process_env(password: str, extra: Optional[dict[str, str]] = None) -> dict[str, str]:
    env = os.environ.copy()
    env['PGPASSWORD'] = password
    if extra:
        env.update(extra)
    return env


def _pgoptions_for_profile(profile: str, maintenance_work_mem_mb: int) -> Optional[str]:
    if profile != 'fast':
        return None

    try:
        memory_mb = int(maintenance_work_mem_mb)
    except (TypeError, ValueError):
        memory_mb = 512
    memory_mb = min(max(64, memory_mb), 8192)
    return f'-c synchronous_commit=off -c maintenance_work_mem={memory_mb}MB'


def enable_turbo_mode(
    pg_bin: str,
    host: str,
    port: str,
    user: str,
    password: str,
    profile: str = 'fast',
    maintenance_work_mem_mb: int = 512,
) -> Tuple[bool, Optional[str], TurboModeContext]:
    normalized = profile.lower().strip()
    if normalized not in {'safe', 'fast', 'unsafe'}:
        return False, f"Unknown turbo mode profile: {profile}", TurboModeContext(profile='safe', env=_build_process_env(password))

    extra_env = {}
    pgoptions = _pgoptions_for_profile(normalized, maintenance_work_mem_mb)
    if pgoptions:
        extra_env['PGOPTIONS'] = pgoptions

    ctx = TurboModeContext(profile=normalized, env=_build_process_env(password, extra_env))
    if normalized in {'safe', 'fast'}:
        return True, None, ctx

    try:
        psql_path = Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql')
        settings_result = subprocess.run(
            [
                str(psql_path), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-tAc',
                "SELECT name, setting FROM pg_settings WHERE name IN ('fsync', 'full_page_writes', 'synchronous_commit') ORDER BY name;"
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=ctx.env,
        )
        if settings_result.returncode != 0:
            return False, settings_result.stderr or settings_result.stdout, ctx

        previous_settings = {}
        for line in settings_result.stdout.strip().splitlines():
            if '|' not in line:
                continue
            name, value = line.split('|', 1)
            previous_settings[name.strip()] = value.strip()
        ctx.previous_settings = previous_settings

        result = subprocess.run(
            [str(psql_path), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-c',
             'ALTER SYSTEM SET fsync = off; ALTER SYSTEM SET full_page_writes = off; ALTER SYSTEM SET synchronous_commit = off; SELECT pg_reload_conf();'],
            capture_output=True,
            text=True,
            timeout=30,
            env=ctx.env,
        )

        if result.returncode == 0:
            return True, None, ctx
        else:
            return False, result.stderr, ctx
    except Exception as e:
        return False, str(e), ctx


def disable_turbo_mode(
    pg_bin: str,
    host: str,
    port: str,
    user: str,
    password: str,
    ctx: Optional[TurboModeContext] = None,
) -> Tuple[bool, Optional[str]]:
    if ctx is None:
        ctx = TurboModeContext(profile='safe', env=_build_process_env(password))
    if ctx.profile in {'safe', 'fast'}:
        return True, None

    try:
        psql_path = Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql')
        previous = ctx.previous_settings or {}
        restore_statements = []
        for name in ('fsync', 'full_page_writes', 'synchronous_commit'):
            value = previous.get(name)
            if value:
                restore_statements.append(f"ALTER SYSTEM SET {name} = '{value}';")
            else:
                restore_statements.append(f'ALTER SYSTEM RESET {name};')

        result = subprocess.run(
            [str(psql_path), '-U', user, '-h', host, '-p', port, '-d', 'postgres', '-c',
             ' '.join(restore_statements) + ' SELECT pg_reload_conf();'],
            capture_output=True,
            text=True,
            timeout=30,
            env=ctx.env,
        )

        return result.returncode == 0, None if result.returncode == 0 else result.stderr
    except Exception as e:
        return False, str(e)


def restore_sql_file(pg_bin: str, host: str, port: str, user: str, password: str, 
                     db_name: str, sql_file: str,
                     process_env: Optional[dict[str, str]] = None,
                     progress_callback: Optional[Callable] = None) -> Tuple[bool, Optional[str], float]:
    env = process_env or _build_process_env(password)

    start_time = datetime.now()
    elapsed = 0.0
    
    try:
        psql_path = Path(pg_bin) / ('psql.exe' if os.name == 'nt' else 'psql')
        
        process = subprocess.Popen(
            [str(psql_path), '-v', 'ON_ERROR_STOP=1', '-U', user, '-h', host, '-p', port, '-d', db_name, '-f', sql_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        
        if progress_callback:
            progress_callback({'status': 'starting', 'message': 'Iniciando restore SQL...'})

        stdout_data, stderr_data = process.communicate()
        elapsed = (datetime.now() - start_time).total_seconds()
        stderr_lines = stderr_data.splitlines() if stderr_data else []

        if progress_callback:
            last_progress_time = 0.0
            for stderr_line in stderr_lines:
                line_lower = stderr_line.lower()
                if 'error' in line_lower:
                    progress_callback({'status': 'error', 'message': stderr_line.strip()})
                    continue

                if any(kw in line_lower for kw in ['create', 'alter', 'insert', 'table', 'index']):
                    current_time = time.time()
                    if current_time - last_progress_time > 0.5:
                        progress_callback({'status': 'progress', 'message': stderr_line.strip(), 'elapsed': elapsed})
                        last_progress_time = current_time
        
        if process.returncode == 0:
            if progress_callback:
                progress_callback({'status': 'complete', 'message': 'SQL restore completed'})
            return True, None, elapsed
        else:
            error_msg = '\n'.join(stderr_lines[-20:])
            if progress_callback:
                progress_callback({'status': 'error', 'message': error_msg})
            return False, error_msg, elapsed
    except subprocess.TimeoutExpired:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, "Restore timed out.", elapsed
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, str(e), elapsed


def restore_custom_dump(pg_bin: str, host: str, port: str, user: str, password: str,
                          db_name: str, dump_file: str, jobs: int = 4,
                          process_env: Optional[dict[str, str]] = None,
                          progress_callback: Optional[Callable] = None) -> Tuple[bool, Optional[str], float]:
    env = process_env or _build_process_env(password)

    start_time = datetime.now()
    elapsed = 0.0
    
    try:
        pg_restore_path = Path(pg_bin) / ('pg_restore.exe' if os.name == 'nt' else 'pg_restore')
        
        args = [
            str(pg_restore_path),
            '-U', user,
            '-h', host,
            '-p', port,
            '-d', db_name,
            '-j', str(jobs),
            '--verbose',
            '--no-owner',
            '--no-acl',
            dump_file
        ]
        
        if progress_callback:
            progress_callback({'status': 'starting', 'message': f'Starting restore with {jobs} jobs...'})
        
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        
        stderr_output = []
        last_progress_time = time.time()
        last_objects_count = 0
        process_stderr = process.stderr
        
        if process_stderr:
            while True:
                line = process_stderr.readline()
                if not line and process.poll() is not None:
                    break
                
                if line:
                    stderr_output.append(line)
                
                line_lower = line.lower()
                
                if 'processing table' in line_lower:
                    current_time = time.time()
                    if current_time - last_progress_time > 3:
                        match = re.search(r'processing table ["\']?(\w+)', line, re.IGNORECASE)
                        table_name = match.group(1) if match else 'unknown'
                        if progress_callback:
                            progress_callback({
                                'status': 'progress',
                                'message': f'Table: {table_name}',
                                'elapsed': (datetime.now() - start_time).total_seconds()
                            })
                        last_progress_time = current_time
                
                elif 'creating index' in line_lower:
                    current_time = time.time()
                    if current_time - last_progress_time > 3:
                        match = re.search(r'creating index ["\']?(\w+)', line, re.IGNORECASE)
                        idx_name = match.group(1) if match else 'unknown'
                        if progress_callback:
                            progress_callback({
                                'status': 'progress',
                                'message': f'Index: {idx_name}',
                                'elapsed': (datetime.now() - start_time).total_seconds()
                            })
                        last_progress_time = current_time
                
                elif 'error' in line_lower:
                    if progress_callback:
                        progress_callback({'status': 'error', 'message': line.strip()})
        
        process.wait()
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if process.returncode == 0:
            if progress_callback:
                progress_callback({
                    'status': 'complete',
                    'message': 'Restore completed',
                    'elapsed': elapsed
                })
            return True, None, elapsed
        else:
            error_msg = '\n'.join(stderr_output[-30:])
            if progress_callback:
                progress_callback({'status': 'error', 'message': error_msg})
            return False, error_msg, elapsed
    except subprocess.TimeoutExpired:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, "Restore timed out.", elapsed
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, str(e), elapsed


def restore_tar_dump(pg_bin: str, host: str, port: str, user: str, password: str,
                     db_name: str, tar_file: str, jobs: int = 4,
                     process_env: Optional[dict[str, str]] = None,
                     progress_callback: Optional[Callable] = None) -> Tuple[bool, Optional[str], float]:
    env = process_env or _build_process_env(password)

    start_time = datetime.now()
    elapsed = 0.0
    
    try:
        pg_restore_path = Path(pg_bin) / ('pg_restore.exe' if os.name == 'nt' else 'pg_restore')
        
        result = subprocess.run(
            [str(pg_restore_path), '-U', user, '-h', host, '-p', port,
             '-d', db_name, '-j', str(jobs), '--verbose', '--no-owner', '--no-acl', tar_file],
            capture_output=True,
            text=True,
            timeout=3600,
            env=env,
        )
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if result.returncode == 0:
            return True, None, elapsed
        else:
            return False, result.stderr, elapsed
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, str(e), elapsed


def restore_directory(pg_bin: str, host: str, port: str, user: str, password: str,
                       db_name: str, backup_dir: str, jobs: int = 4,
                       process_env: Optional[dict[str, str]] = None,
                       progress_callback: Optional[Callable] = None) -> Tuple[bool, Optional[str], float]:
    env = process_env or _build_process_env(password)

    start_time = datetime.now()
    elapsed = 0.0
    
    try:
        pg_restore_path = Path(pg_bin) / ('pg_restore.exe' if os.name == 'nt' else 'pg_restore')
        
        result = subprocess.run(
            [str(pg_restore_path), '-U', user, '-h', host, '-p', port,
             '-d', db_name, '-j', str(jobs), '--verbose', '--no-owner', '--no-acl', backup_dir],
            capture_output=True,
            text=True,
            timeout=3600,
            env=env,
        )
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if result.returncode == 0:
            return True, None, elapsed
        else:
            return False, result.stderr, elapsed
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        return False, str(e), elapsed


def get_restore_command(pg_bin: str, backup_type: str, host: str, port: str, user: str,
                        db_name: str, backup_path: str, jobs: int = 4) -> list:
    is_windows = os.name == 'nt'
    psql_bin = 'psql.exe' if is_windows else 'psql'
    pg_restore_bin = 'pg_restore.exe' if is_windows else 'pg_restore'
    
    if backup_type == 'sql':
        return [str(Path(pg_bin) / psql_bin), '-v', 'ON_ERROR_STOP=1', '-U', user, '-h', host, '-p', port, '-d', db_name, '-f', backup_path]
    else:
        base = [str(Path(pg_bin) / pg_restore_bin), '-U', user, '-h', host, '-p', port, '-d', db_name]
        base.extend(['-j', str(jobs), '--no-owner', '--no-acl', backup_path])
    
    return base


def parse_restore_progress(line: str) -> Optional[dict]:
    if 'processing table' in line.lower():
        match = re.search(r'processing table ["\']?(\w+)', line, re.IGNORECASE)
        if match:
            return {'type': 'table', 'name': match.group(1)}
    elif 'creating index' in line.lower():
        match = re.search(r'creating index ["\']?(\w+)', line, re.IGNORECASE)
        if match:
            return {'type': 'index', 'name': match.group(1)}
    
    return None


if __name__ == '__main__':
    print("Restore engine utilities with real-time progress")
