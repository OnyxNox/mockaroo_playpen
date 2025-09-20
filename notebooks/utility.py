from enum import Enum
import logging
import random
import re
import time


class SchemaHealth(Enum):
    HEALTHY = "Healthy"
    UNHEALTHY = "Unhealthy"


class SchemaType(Enum):
    GENERATED = "Generated"
    STATIC = "Static"


def get_mockaroo_schema_name_metadata(mockaroo_schema_name):
    match = re.match(r"\[([HU]):([GS])\]\s*(.+)", mockaroo_schema_name)
    schema_health, schema_type, schema_name = match.groups()

    schema_health = (
        SchemaHealth.HEALTHY if schema_health == "H" else SchemaHealth.UNHEALTHY
    )
    schema_type = SchemaType.GENERATED if schema_type == "G" else SchemaType.STATIC

    return (schema_health, schema_type, schema_name)


def init_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    return logging.getLogger(__name__)


def sleep_with_jitter(logger):
    """Sleep for a random duration between 0.5-2.5 seconds"""
    jitter = random.uniform(0.5, 2.5)
    logger.info(f"💤 Sleeping for {jitter:.2f} seconds...")
    time.sleep(jitter)
