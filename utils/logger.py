import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional


LOG_DIR = Path.home() / '.pg_restore_logs'
LOG_FILE = LOG_DIR / 'restore.log'


def setup_logger(name: str = 'pg_restore') -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
        handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger


def log_operation(operation: str, details: str = '', level: str = 'info') -> None:
    logger = setup_logger()
    
    msg = f"{operation}: {details}" if details else operation
    
    if level == 'error':
        logger.error(msg)
    elif level == 'warning':
        logger.warning(msg)
    else:
        logger.info(msg)


def mask_password(password: str) -> str:
    if not password:
        return ''
    return '*' * min(len(password), 8)


def log_connection_attempt(host: str, port: str, user: str) -> None:
    log_operation('CONNECTION', f"{user}@{host}:{port}")


def log_restore_start(backup_file: str, target_db: str) -> None:
    log_operation('RESTORE_START', f"{backup_file} -> {target_db}")


def log_restore_end(success: bool, duration: float, tables_count: int = 0) -> None:
    status = 'SUCESSO' if success else 'FALHA'
    log_operation('RESTORE_END', f"{status} - {duration:.1f}s - {tables_count} tabelas")


if __name__ == '__main__':
    print("Logger utilities")