#!/usr/bin/env python3
from datetime import datetime
import logging
import os
import uuid


def get_run_id(args_run_id) -> str:
    """
    Get or generate a unique run identifier.

    Args:
        args_run_id: Run ID provided as a CLI argument (optional).

    Returns:
        str: Run ID from CLI arguments, ROO_BRICKS_RUN_ID environment variable, or auto-generated timestamp-UUID format
             (YYYYMMDD-HHMMSS-uuid).
    """
    return (
        args_run_id
        or os.getenv("ROO_BRICKS_RUN_ID")
        or f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}"
    )


def init_logger(module_name: str, log_level: str, run_output_path: str):
    """
    Initialize and configure the logger with file and console handlers.

    Args:
        module_name: Name of the module for the logger.
        log_level: Logging level (e.g., 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
        run_output_path: Directory path where the log file will be created.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[logging.FileHandler(os.path.join(run_output_path, "roo_bricks.log")), logging.StreamHandler()],
    )

    return logging.getLogger(module_name)
