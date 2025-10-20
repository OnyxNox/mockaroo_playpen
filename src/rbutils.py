#!/usr/bin/env python3
from abc import ABC, abstractmethod
from datetime import datetime
import logging
import os
import uuid


# ===== Secrets Abstraction =====
class SecretsManager(ABC):
    """Abstract base class for secrets management"""

    @abstractmethod
    def get(self, scope: str, key: str) -> str:
        """Get a secret value"""
        pass


class LocalSecretsManager(SecretsManager):
    """Local implementation using environment variables"""

    def get(self, scope: str, key: str) -> str:
        # Convert scope-key to environment variable name format
        # e.g., "roo-bricks" + "mockaroo-api-key" -> "ROO_BRICKS_MOCKAROO_API_KEY"
        env_var_name = f"{scope}_{key}".upper().replace("-", "_")
        value = os.getenv(env_var_name)

        if value is None:
            raise ValueError(f"Secret not found in environment variables: {env_var_name}")

        return value


# ===== Lazy Initialization =====
_secrets_manager = None


def _get_secrets_manager() -> SecretsManager:
    """Lazy initialization of the secrets manager"""
    global _secrets_manager

    if _secrets_manager is None:
        _secrets_manager = LocalSecretsManager()

    return _secrets_manager


# ===== Public API =====
class secrets:
    """Secrets interface using environment variables"""

    @staticmethod
    def get(scope: str, key: str) -> str:
        """
        Get a secret value from environment variables.

        The secret is read from environment variable {SCOPE}_{KEY} (uppercase, hyphens replaced with underscores).

        Args:
            scope: Secret scope (e.g., 'roo-bricks')
            key: Secret key (e.g., 'mockaroo-api-key')

        Returns:
            str: The secret value

        Example:
            api_key = rbutils.secrets.get('roo-bricks', 'mockaroo-api-key')
            # Reads os.getenv('ROO_BRICKS_MOCKAROO_API_KEY')
        """
        return _get_secrets_manager().get(scope, key)


# ===== Existing Utility Functions =====
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
    os.makedirs(run_output_path, exist_ok=True)

    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(run_output_path, "roo_bricks.log"))
    ]

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=handlers,
    )

    return logging.getLogger(module_name)
