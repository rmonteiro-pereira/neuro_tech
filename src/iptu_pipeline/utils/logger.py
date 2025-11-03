"""
Logging utility for IPTU pipeline.
Consolidated and efficient logging system with centralized configuration.
"""
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
from threading import Lock

from iptu_pipeline.config import settings

# Thread-safe singleton pattern
_logger_lock = Lock()
_root_logger_initialized = False
_log_file_path: Optional[Path] = None


def _get_log_level() -> str:
    """Get log level from environment variable or settings."""
    # Check environment variable first (IPTU_LOG_LEVEL)
    log_level = os.getenv("IPTU_LOG_LEVEL", "").upper()
    if log_level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        return log_level
    # Fallback to INFO if not set
    return "INFO"


def _setup_root_logger() -> None:
    """
    Setup root logger with file and console handlers.
    This is called once and all child loggers inherit from it.
    """
    global _root_logger_initialized, _log_file_path
    
    with _logger_lock:
        if _root_logger_initialized:
            return
        
        root_logger = logging.getLogger("iptu_pipeline")
        
        # Clear any existing handlers to avoid duplicates
        root_logger.handlers.clear()
        root_logger.propagate = False
        
        # Set log level
        log_level = _get_log_level()
        root_logger.setLevel(getattr(logging, log_level))
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        simple_formatter = logging.Formatter(
            '%(levelname)s - %(message)s'
        )
        
        # File handler - single file per application run
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        _log_file_path = settings.LOG_DIR / f"iptu_pipeline_{timestamp}.log"
        
        file_handler = logging.FileHandler(_log_file_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # File always has DEBUG level
        file_handler.setFormatter(detailed_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level))
        console_handler.setFormatter(simple_formatter)
        
        # Add handlers
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        
        _root_logger_initialized = True


def setup_logger(name: Optional[str] = None, log_level: Optional[str] = None) -> logging.Logger:
    """
    Setup logger with centralized configuration.
    
    All loggers are children of the root 'iptu_pipeline' logger,
    which ensures a single log file per application run.
    
    Args:
        name: Logger name (module name). If None, uses 'iptu_pipeline'.
              If provided, becomes 'iptu_pipeline.module_name'
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                   If None, uses environment variable IPTU_LOG_LEVEL or defaults to INFO.
    
    Returns:
        Configured logger instance
    
    Examples:
        >>> logger = setup_logger()  # Root logger
        >>> logger = setup_logger("engine")  # Child logger: iptu_pipeline.engine
        >>> logger = setup_logger("pipelines.main_pipeline")  # Nested child logger
    """
    # Initialize root logger if not already done
    _setup_root_logger()
    
    # Build logger name using hierarchy
    if name is None or name == "iptu_pipeline":
        logger_name = "iptu_pipeline"
    else:
        # Create child logger: iptu_pipeline.module_name
        logger_name = f"iptu_pipeline.{name}"
    
    logger = logging.getLogger(logger_name)
    
    # Set log level if provided, otherwise inherit from root
    if log_level:
        logger.setLevel(getattr(logging, log_level.upper()))
    else:
        # Inherit from root logger
        logger.setLevel(logging.NOTSET)
    
    # Ensure logger doesn't create duplicate handlers
    # Child loggers propagate to root, so we don't need separate handlers
    logger.propagate = True
    
    return logger


def get_log_file_path() -> Optional[Path]:
    """
    Get the path to the current log file.
    
    Returns:
        Path to log file, or None if logger hasn't been initialized
    """
    return _log_file_path


def set_log_level(level: str) -> None:
    """
    Dynamically change the log level for all loggers.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level_upper = level.upper()
    if level_upper not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        raise ValueError(f"Invalid log level: {level}")
    
    root_logger = logging.getLogger("iptu_pipeline")
    root_logger.setLevel(getattr(logging, level_upper))
    
    # Update console handler level
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handler.setLevel(getattr(logging, level_upper))


# Convenience function for backward compatibility
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Alias for setup_logger for backward compatibility.
    
    Args:
        name: Logger name (module name)
    
    Returns:
        Configured logger instance
    """
    return setup_logger(name)
