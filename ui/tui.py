import os
import sys
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import print as rprint


console = Console()


def print_header():
    console.print(Panel.fit(
        "[bold cyan]PG Restore Tool[/bold cyan]\n"
        "Fast PostgreSQL restore utility",
        border_style="cyan",
        padding=(1, 2)
    ))


def print_env_check(env_result):
    console.print("\n[bold]Phase 0 - Environment Check[/bold]")
    console.print(f"  OS: {env_result.os_version}")
    console.print(f"  UTF-8 Terminal: {'[green]OK[/green]' if env_result.terminal_utf8 else '[red]FAIL[/red]'}")
    console.print(f"  PostgreSQL: {len(env_result.pg_installations)} installation(s)")
    
    if env_result.pg_installations:
        console.print("  Binaries found:")
        for pg in env_result.pg_installations:
            tools = env_result.pg_tools_available.get(pg, {})
            tools_str = ', '.join([t for t, ok in tools.items() if ok])
            console.print(f"    - {pg}: {tools_str or 'none'}")
    
    if env_result.warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in env_result.warnings:
            console.print(f"  - {w}")


def print_capabilities(cap):
    table = Table(title="Capability Matrix", show_header=True)
    table.add_column("Capability", style="cyan")
    table.add_column("Status", justify="center")
    
    table.add_row("Restore (custom)", "[green]OK[/green]" if cap.can_restore_custom else "[red]NO[/red]")
    table.add_row("Restore (tar)", "[green]OK[/green]" if cap.can_restore_tar else "[red]NO[/red]")
    table.add_row("Restore (directory)", "[green]OK[/green]" if cap.can_restore_dir else "[red]NO[/red]")
    table.add_row("Restore (SQL)", "[green]OK[/green]" if cap.can_restore_sql else "[red]NO[/red]")
    table.add_row("Pre-restore backup", "[green]OK[/green]" if cap.can_backup_before_restore else "[red]NO[/red]")
    table.add_row("Parallel restore", "[green]OK[/green]" if cap.can_parallel_restore else "[red]NO[/red]")
    table.add_row("Max parallel jobs", str(cap.max_parallel_jobs))
    table.add_row("Keyring", "[green]OK[/green]" if cap.can_use_keyring else "[red]NO[/red]")
    table.add_row("PG versions", ', '.join(cap.supported_pg_versions) or "None")
    
    console.print(table)


def print_backup_list(backups):
    if not backups:
        console.print("[yellow]No backup files found.[/yellow]")
        return
    
    table = Table(title=f"Backups Found ({len(backups)})", show_header=True)
    table.add_column("#", justify="right", width=3)
    table.add_column("Name", style="cyan")
    table.add_column("Type", width=10)
    table.add_column("Size", justify="right", width=10)
    table.add_column("Date", width=16)
    table.add_column("Status", width=20)
    
    for i, b in enumerate(backups, 1):
        size_str = f"{b.size_mb:.1f} MB"
        from datetime import datetime
        date_str = datetime.fromtimestamp(b.modified).strftime('%Y-%m-%d %H:%M')
        
        status = ""
        if b.is_suspicious:
            status = "[red]WARNING[/red]"
        elif b.source_db:
            status = f"DB: {b.source_db}"
        
        table.add_row(str(i), b.name[:40], b.backup_type, size_str, date_str, status)
    
    console.print(table)


def print_instances(instances):
    if not instances:
        console.print("[yellow]No PostgreSQL instances found.[/yellow]")
        return
    
    table = Table(title="PostgreSQL Instances", show_header=True)
    table.add_column("#", justify="right", width=3)
    table.add_column("Host:Port", style="cyan")
    table.add_column("Service", width=20)
    table.add_column("Status", justify="center")
    
    for i, inst in enumerate(instances, 1):
        status_icon = "●" if inst.status == 'running' else "○"
        status_style = "green" if inst.status == 'running' else "yellow"
        
        table.add_row(str(i), f"{inst.host}:{inst.port}", inst.service_name or "N/A", f"[{status_style}]{status_icon}[/{status_style}]")
    
    console.print(table)


def print_databases(databases):
    if not databases:
        console.print("[yellow]No databases found.[/yellow]")
        return
    
    table = Table(title="Databases", show_header=True)
    table.add_column("#", justify="right", width=3)
    table.add_column("Name", style="cyan")
    table.add_column("Size", justify="right", width=12)
    
    for i, db in enumerate(databases, 1):
        size_str = f"{db['size_mb']:.1f} MB"
        table.add_row(str(i), db['name'], size_str)
    
    console.print(table)


def prompt_yes_no(message: str) -> bool:
    response = console.input(f"{message} [y/N]: ")
    return response.strip().lower() == 'y'


def prompt_choice(message: str, options: list) -> int:
    for i, opt in enumerate(options, 1):
        console.print(f"  [{i}] {opt}")
    
    while True:
        try:
            choice = console.input("  Escolha: ")
            
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return idx
        except ValueError:
            pass


def print_restore_progress(line: str, elapsed: float = 0):
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    time_str = f"{mins:02d}:{secs:02d}" if mins > 0 else f"{secs}s"
    console.print(f"  [{time_str}] [cyan]{line.strip()}[/cyan]")


def print_space_check(space_data: dict, message: str = ""):
    free_mb = space_data.get('free_mb', 0)
    required_mb = space_data.get('required_mb', 0)
    total_gb = space_data.get('total_gb', 0)
    backup_size = space_data.get('backup_size_mb', 0)
    current_db = space_data.get('current_db_size_mb', 0)
    
    console.print(Panel.fit(
        f"[bold]Disk Space Check[/bold]\n\n"
        f"Backup file:          {backup_size:.1f} MB\n"
        f"Estimated restore:    ~{required_mb:.1f} MB (3x factor)\n"
        f"Free space (PGDATA):  {free_mb:.1f} MB\n"
        f"Current DB size:      {current_db:.1f} MB\n"
        f"Disk total:           {total_gb:.1f} GB\n\n"
        f"{message or ''}",
        border_style="cyan" if not message or "insufficient" not in message.lower() else "yellow",
        padding=(1, 2)
    ))


def print_restore_summary(success: bool, duration: float, tables: int, views: int, functions: int, warnings: list):
    if success:
        console.print(Panel.fit(
            f"[green]RESTORE COMPLETED SUCCESSFULLY[/green]\n\n"
            f"Tables: {tables}\n"
            f"Views: {views}\n"
            f"Functions: {functions}\n"
            f"Duration: {duration:.1f}s",
            border_style="green",
            padding=(1, 2)
        ))
    else:
        console.print(Panel.fit(
            "[red]RESTORE FAILED[/red]",
            border_style="red",
            padding=(1, 2)
        ))
    
    if warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in warnings:
            console.print(f"  - {w}")


def print_error(message: str):
    console.print(f"[red]Error:[/red] {message}")


def print_warning(message: str):
    console.print(f"[yellow]Warning:[/yellow] {message}")


def print_info(message: str):
    console.print(f"[cyan]Info:[/cyan] {message}")


if __name__ == '__main__':
    print_header()
