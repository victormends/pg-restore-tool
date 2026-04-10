import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Any

from core.env_check import run_env_check, EnvCheckResult
from core.capability_matrix import build_capability_matrix, print_capability_report, CapabilityMatrix
from core.backup_scanner import scan_backup_dirs, BackupFile
from core.pg_detector import detect_pg_instances
from core.connection import test_connection, ConnectionResult
from core.database_ops import (
    list_databases, create_database, drop_database, get_data_directory,
    get_disk_space, check_space_for_restore, get_database_size
)
from core.restore_engine import (
    enable_turbo_mode, disable_turbo_mode, restore_sql_file, restore_custom_dump,
    restore_tar_dump, restore_directory, TurboModeContext
)
from core.post_restore import (
    run_analyze, verify_tables_count, verify_views_count,
    verify_functions_count, sanity_check
)
from core.backup_engine import (
    backup_database, generate_backup_filename, verify_backup_file,
    pg_dump_available, cleanup_old_backups
)
from core.config import load_config, print_config
from profiles.profile_manager import check_pgpass, match_pgpass_entry, get_env_password
from ui.tui import (
    print_header, print_env_check, print_capabilities, print_backup_list,
    print_instances, print_databases, prompt_yes_no, print_restore_summary,
    print_error, print_warning, print_info, print_space_check, print_restore_progress
)


EXIT_CODES = {
    0: 'Success',
    1: 'Generic error',
    2: 'Insufficient disk space',
    3: 'PG version incompatibility',
    4: 'Authentication / connection failure',
    5: 'Invalid or corrupted backup file',
    6: 'Target database unavailable',
    7: 'Restore interrupted by user',
    8: 'Post-restore failure',
}


class RestoreContext:
    pg_bin: Optional[str]
    env_result: Optional[EnvCheckResult]
    cap: Optional[CapabilityMatrix]
    backups: list[BackupFile]
    selected_backup: Optional[BackupFile]
    instances: list[Any]
    selected_instance: Optional[Any]
    connection_result: Optional[ConnectionResult]
    databases: list[dict]
    target_db: Optional[str]
    backup_path: Optional[str]
    start_time: Optional[datetime]
    result_data: dict
    password: Optional[str]
    turbo_context: Optional[TurboModeContext]

    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.pg_bin: Optional[str] = None
        self.env_result: Optional[EnvCheckResult] = None
        self.cap: Optional[CapabilityMatrix] = None
        self.backups: list[BackupFile] = []
        self.selected_backup: Optional[BackupFile] = None
        self.instances: list[Any] = []
        self.selected_instance: Optional[Any] = None
        self.connection_result: Optional[ConnectionResult] = None
        self.databases: list[dict] = []
        self.target_db: Optional[str] = None
        self.backup_path: Optional[str] = None
        self.start_time: Optional[datetime] = None
        self.result_data: dict = {}
        self.password: Optional[str] = None
        self.turbo_context: Optional[TurboModeContext] = None
    
    def to_json(self) -> dict:
        return {
            'success': self.result_data.get('success', False),
            'backup': self.result_data.get('backup_file'),
            'target_db': self.result_data.get('target_db'),
            'host': self.result_data.get('host'),
            'port': self.result_data.get('port'),
            'duration': self.result_data.get('duration'),
            'tables': self.result_data.get('tables'),
            'views': self.result_data.get('views'),
            'functions': self.result_data.get('functions'),
            'warnings': self.result_data.get('warnings', []),
            'error': self.result_data.get('error'),
            'timestamp': datetime.now().isoformat(),
        }


def run_interactive(ctx: RestoreContext) -> int:
    print_header()
    
    print_info("Phase 0 - Environment check...")
    ctx.env_result = run_env_check()
    print_env_check(ctx.env_result)
    
    ctx.cap = build_capability_matrix(ctx.env_result)
    print_capabilities(ctx.cap)
    assert ctx.env_result is not None
    assert ctx.cap is not None
    
    if not ctx.env_result.pg_installations:
        print_error("No PostgreSQL installation found. Exiting.")
        return 1
    
    ctx.pg_bin = ctx.env_result.pg_installations[0]
    pg_bin = ctx.pg_bin
    
    print_info("Phase 1 - Scanning backups...")
    scan_dirs = [Path(d) for d in ctx.config.backup_dirs]
    ctx.backups = scan_backup_dirs(dirs=scan_dirs, pg_bin=pg_bin)
    print_backup_list(ctx.backups)
    
    if not ctx.backups:
        print_warning("No backups found.")
    
    selected_backup = None
    if ctx.args.file:
        for b in ctx.backups:
            if ctx.args.file in b.path or ctx.args.file in b.name:
                selected_backup = b
                break
        if not selected_backup:
            print_error(f"Backup '{ctx.args.file}' not found.")
            return 5
    elif ctx.backups:
        print_info("Select the backup file:")
        for i, b in enumerate(ctx.backups, 1):
            print(f"  [{i}] {b.name}")
        
        choice = input("  Choice: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(ctx.backups):
            selected_backup = ctx.backups[int(choice) - 1]
    
    if not selected_backup:
        print_error("No backup selected.")
        return 1
    
    ctx.selected_backup = selected_backup
    print_info(f"Selected backup: {selected_backup.name}")
    
    print_info("Phase 2 - Detecting PostgreSQL instances...")
    ctx.instances = detect_pg_instances()
    print_instances(ctx.instances)
    
    if not ctx.instances:
        print_error("No PostgreSQL instance found.")
        return 6
    
    host = ctx.args.host
    port = ctx.args.port
    
    password = ctx.args.password
    if not password:
        pgpass = check_pgpass()
        if pgpass:
            matched = match_pgpass_entry(pgpass, host, port, '*', ctx.args.user)
            if matched:
                password = matched
                print_info("Password loaded from .pgpass")
        
        if not password:
            password = get_env_password()
            if password:
                print_info("Password loaded from PGPASSWORD")
        
        if not password:
            import getpass
            password = getpass.getpass("Password: ")
    
    ctx.password = password
    
    print_info(f"Connecting to {host}:{port}...")
    ctx.connection_result = test_connection(pg_bin, host, port, ctx.args.user, password)
    assert ctx.connection_result is not None
    
    if not ctx.connection_result.success:
        print_error(f"Could not connect: {ctx.connection_result.error}")
        return 4
    
    print_info(f"Connection OK! (SSL: {ctx.connection_result.ssl_mode}, Superuser: {ctx.connection_result.is_superuser})")
    
    print_info("Phase 3 - Listing databases...")
    ctx.databases = list_databases(pg_bin, host, port, ctx.args.user, password)
    print_databases(ctx.databases)
    
    target_db = ctx.args.db
    
    if not target_db and ctx.databases:
        print_info("Select the target database:")
        for i, db in enumerate(ctx.databases, 1):
            print(f"  [{i}] {db['name']}")
        print(f"  [{len(ctx.databases)+1}] Create new database")
        
        choice = input("  Choice: ").strip()
        if choice.isdigit():
            idx = int(choice) - 1
            if idx < len(ctx.databases):
                target_db = ctx.databases[idx]['name']
            elif idx == len(ctx.databases):
                target_db = input("  New database name: ").strip()
    
    if not target_db:
        print_error("Target database not specified.")
        return 1
    
    ctx.target_db = target_db
    
    print_info("Phase 4 - Checking disk space...")
    db_exists = any(db['name'] == target_db for db in ctx.databases)
    space_ok, space_msg, space_data = check_space_for_restore(
        ctx.pg_bin, host, port, ctx.args.user, password,
        selected_backup.size_mb, target_db, drop_existing=db_exists
    )
    
    print_space_check(space_data, space_msg or "")
    
    if not space_ok:
        print_error(f"Insufficient space: {space_msg}")
        if not prompt_yes_no("Continue anyway?"):
            return 2
    
    backup_before = False
    can_backup = ctx.cap.can_backup_before_restore and pg_dump_available(pg_bin)
    
    if db_exists and can_backup and not ctx.config.skip_backup:
        if prompt_yes_no("Create a safety backup before restoring?"):
            backup_before = True
    
    if backup_before:
        print_info("Phase 4.5 - Creating safety backup...")
        backup_filename = generate_backup_filename(target_db)
        backup_dir = Path.home() / 'Desktop' / 'pg_restore_backups'
        backup_dir.mkdir(exist_ok=True)
        ctx.backup_path = str(backup_dir / backup_filename)
        
        print_info(f"Creating backup at {ctx.backup_path}...")
        success, error, duration = backup_database(
            pg_bin, host, port, ctx.args.user, password,
            target_db, ctx.backup_path,
            process_env=ctx.turbo_context.env if ctx.turbo_context else None,
        )
        
        if success:
            print_info(f"Safety backup created ({duration:.1f}s)")
            cleanup_old_backups(str(backup_dir), keep_last=5)
        else:
            print_warning(f"Safety backup failed: {error}")
            if not prompt_yes_no("Continue without backup?"):
                return 1
    
    if ctx.args.dry_run:
        print_info(f"[DRY RUN] Restore {selected_backup.name} -> {target_db}")
        return 0
    
    print_info("Phase 4.6 - Validating backup file...")
    if selected_backup.backup_type != 'sql':
        valid, error = verify_backup_file(selected_backup.path, pg_bin)
        if not valid:
            print_error(f"Invalid backup file: {error}")
            return 5
        print_info("Backup file is valid ✓")
    
    recreate_existing = False
    if db_exists:
        recreate_existing = prompt_yes_no(f"Database '{target_db}' already exists. Drop and recreate it?")
        if recreate_existing:
            print_info(f"Dropping database '{target_db}'...")
            ok, err = drop_database(pg_bin, host, port, ctx.args.user, password, target_db, force=True)
            if not ok:
                print_error(f"Error dropping database: {err}")
                return 6

    if recreate_existing or not db_exists or ctx.args.create_new:
        print_info(f"Creating database '{target_db}'...")
        ok, err = create_database(pg_bin, host, port, ctx.args.user, password, target_db)
        if not ok:
            print_error(f"Error creating database: {err}")
            return 6

    print_info(f"Phase 5 - Preparing restore profile ({ctx.config.turbo_mode})...")
    if ctx.config.turbo_mode == 'unsafe':
        print_warning('Unsafe mode changes cluster-wide durability settings for the whole PostgreSQL instance.')
        if not prompt_yes_no('Enable unsafe mode?'):
            return 7

    turbo_enabled, turbo_error, turbo_context = enable_turbo_mode(
        pg_bin,
        host,
        port,
        ctx.args.user,
        password,
        profile=ctx.config.turbo_mode,
        maintenance_work_mem_mb=ctx.config.maintenance_work_mem_mb,
    )
    ctx.turbo_context = turbo_context
    if not turbo_enabled:
        print_error(f"Failed to prepare restore profile: {turbo_error}")
        return 1
    
    print_info("Phase 6 - Restoring...")
    ctx.start_time = datetime.now()
    
    jobs = ctx.args.jobs or ctx.config.default_jobs
    
    def progress_handler(data: dict):
        status = data.get('status', '')
        message = data.get('message', '')
        elapsed = data.get('elapsed', 0)
        
        if status == 'starting':
            print_info(message)
        elif status == 'progress':
            print_restore_progress(message, elapsed)
        elif status == 'complete':
            print_info(message)
        elif status == 'error':
            print_error(message)
    
    success = False
    error = None
    duration = 0.0
    try:
        if selected_backup.backup_type == 'sql':
            success, error, duration = restore_sql_file(
                pg_bin, host, port, ctx.args.user, password,
                target_db, selected_backup.path,
                process_env=ctx.turbo_context.env if ctx.turbo_context else None,
                progress_callback=progress_handler
            )
        elif selected_backup.backup_type in ('custom', 'unknown'):
            success, error, duration = restore_custom_dump(
                pg_bin, host, port, ctx.args.user, password,
                target_db, selected_backup.path, jobs,
                process_env=ctx.turbo_context.env if ctx.turbo_context else None,
                progress_callback=progress_handler
            )
        elif selected_backup.backup_type == 'tar':
            success, error, duration = restore_tar_dump(
                pg_bin, host, port, ctx.args.user, password,
                target_db, selected_backup.path, jobs,
                process_env=ctx.turbo_context.env if ctx.turbo_context else None,
                progress_callback=progress_handler
            )
        elif selected_backup.backup_type == 'directory':
            success, error, duration = restore_directory(
                pg_bin, host, port, ctx.args.user, password,
                target_db, selected_backup.path, jobs,
                process_env=ctx.turbo_context.env if ctx.turbo_context else None,
                progress_callback=progress_handler
            )
        else:
            error = f"Backup type not supported for fast restore: {selected_backup.backup_type}"
    finally:
        print_info("Cleaning up restore profile...")
        turbo_disabled, disable_error = disable_turbo_mode(pg_bin, host, port, ctx.args.user, password, ctx.turbo_context)
        if not turbo_disabled:
            print_warning(f"Failed to clean up restore profile: {disable_error}")
    
    print_info("Phase 7 - Post-restore...")
    post_restore_env = ctx.turbo_context.env if ctx.turbo_context else None
    analyze_ok, analyze_err = run_analyze(pg_bin, host, port, ctx.args.user, password, target_db, process_env=post_restore_env)
    
    tables = verify_tables_count(pg_bin, host, port, ctx.args.user, password, target_db, process_env=post_restore_env)
    views = verify_views_count(pg_bin, host, port, ctx.args.user, password, target_db, process_env=post_restore_env)
    functions = verify_functions_count(pg_bin, host, port, ctx.args.user, password, target_db, process_env=post_restore_env)
    
    sane, sane_err = sanity_check(pg_bin, host, port, ctx.args.user, password, target_db, process_env=post_restore_env)
    
    ctx.result_data = {
        'success': success,
        'backup_file': selected_backup.name,
        'target_db': target_db,
        'host': host,
        'port': port,
        'duration': duration,
        'tables': tables,
        'views': views,
        'functions': functions,
        'warnings': [],
    }
    
    if error:
        ctx.result_data['warnings'].append(error)
        ctx.result_data['error'] = error
    if not analyze_ok and analyze_err:
        ctx.result_data['warnings'].append(f"ANALYZE failed: {analyze_err}")
    if not sane and sane_err:
        ctx.result_data['warnings'].append(f"Sanity check failed: {sane_err}")
    
    print_restore_summary(success, duration, tables, views, functions, ctx.result_data['warnings'])
    
    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(description='PG Restore Tool - Fast PostgreSQL restore utility')
    parser.add_argument('--file', '-f', help='Backup file to restore')
    parser.add_argument('--host', default='127.0.0.1', help='PostgreSQL host (default: 127.0.0.1)')
    parser.add_argument('--port', default='5432', help='PostgreSQL port (default: 5432)')
    parser.add_argument('--user', default='postgres', help='PostgreSQL user (default: postgres)')
    parser.add_argument('--password', help='PostgreSQL password')
    parser.add_argument('--db', '--database', help='Target database name')
    parser.add_argument('--create-new', action='store_true', help='Create a new database if needed')
    parser.add_argument('--jobs', type=int, help='Number of parallel jobs')
    parser.add_argument('--json', action='store_true', help='JSON output')
    parser.add_argument('--dry-run', action='store_true', help='Simulate without executing')
    parser.add_argument('--config', help='Custom configuration file')
    parser.add_argument('--skip-backup', action='store_true', help='Skip automatic pre-restore backup')
    parser.add_argument('--turbo-mode', choices=['safe', 'fast', 'unsafe'], help='Restore profile (default from config)')
    
    args = parser.parse_args()
    
    config = load_config(args.config)
    config.skip_backup = config.skip_backup or args.skip_backup
    if args.turbo_mode:
        config.turbo_mode = args.turbo_mode
    
    ctx = RestoreContext(args, config)
    
    try:
        exit_code = run_interactive(ctx)
        
        if args.json:
            print(json.dumps(ctx.to_json(), indent=2, ensure_ascii=False))
        
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print_warning("\nOperation cancelled by user.")
        sys.exit(7)
    except Exception as e:
        import traceback
        print_error(f"Unexpected error: {str(e)}")
        if args.json:
            print(json.dumps({'error': str(e), 'trace': traceback.format_exc()}, indent=2))
        sys.exit(1)


if __name__ == '__main__':
    main()
