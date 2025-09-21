from databricks.sdk.runtime import *
import json
import pandas as pd
from pyspark.sql.functions import current_timestamp, col, to_timestamp
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from datetime import datetime
from utility import get_mockaroo_schema_name_metadata, init_logger


class SourceLoader:
    def __init__(self):
        self.logger = init_logger()

    def setup_widgets(self):
        # Remove old widgets
        dbutils.widgets.removeAll()

        dbutils.widgets.text("CATALOG_NAME", "roo_bricks", "Catalog Name")
        dbutils.widgets.text(
            "DATABRICKS_SCHEMA_NAME", "mockaroo_data", "Databricks Schema Name"
        )
        dbutils.widgets.text("VOLUME_NAME", "raw_data", "Volume Name")

        # Load Mockaroo schema endpoint map
        with open("../data/mockaroo_schema_endpoint_map.json", "r") as f:
            schema_endpoint_map = json.load(f)

        schema_names = list(schema_endpoint_map.keys())
        default_schema_name = schema_names[0]
        dbutils.widgets.dropdown(
            "MOCKAROO_SCHEMA_NAME",
            default_schema_name,
            schema_names,
            "!Mockaroo Schema Name",
        )

    def load(self):
        try:
            catalog_name = dbutils.widgets.get("CATALOG_NAME")
            databricks_schema_name = dbutils.widgets.get("DATABRICKS_SCHEMA_NAME")
            mockaroo_schema_name = dbutils.widgets.get("MOCKAROO_SCHEMA_NAME")
            volume_name = dbutils.widgets.get("VOLUME_NAME")

            (schema_health, schema_type, schema_name) = (
                get_mockaroo_schema_name_metadata(mockaroo_schema_name)
            )

            schema_display_name = f"{schema_health.value} {schema_name}"

            entity_name = f"{schema_type.value.lower()}_{schema_health.value.lower()}_{schema_name.lower()}"
            source_files_path = f"/Volumes/{catalog_name}/{databricks_schema_name}/{volume_name}/{entity_name}/*.jsonl"
            target_table_name = (
                f"{catalog_name}.{databricks_schema_name}.src_{entity_name}"
            )

            self.logger.info(f"📁 source dataset: {source_files_path}")
            self.logger.info(f"🎯 target table: {target_table_name}")

            self.logger.info("🎣 Casting Strings to Data Frame Types...")
            df = self._cast_to_dataframe_types(spark.read.json(source_files_path))

            df = df.withColumn(
                "__m_ingestion_timestamp", current_timestamp()
            ).withColumn("__m_source_file", col("_metadata.file_path"))

            self.logger.info(f"🔄 Refreshing Target Table...")
            self.logger.info(f"📊 source record count: {df.count()}")
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(target_table_name)

            self.logger.info(f"✅ successfully processed: {schema_display_name}")

        except Exception as e:
            self.logger.error(f"❌ unexpected error: {e}")

    def _cast_to_dataframe_types(self, df):
        if df.count() == 0:
            self.logger.warning("DataFrame is empty, returning original DataFrame")
            return df

        # Get all records
        all_rows = df.collect()

        # Build list of columns to cast
        cast_columns = []

        for field in df.schema.fields:
            if field.dataType != StringType():
                continue

            # Collect all non-null values for this field from all records
            field_values = [row[field.name] for row in all_rows if row[field.name] is not None]

            if not field_values:
                continue

            # Cast each value and count the types
            type_counts = {}
            for field_value in field_values:
                casted_value = self._cast_to_python_type(field_value)

                if isinstance(casted_value, bool):
                    type_key = 'bool'
                elif isinstance(casted_value, int):
                    type_key = 'int'
                elif isinstance(casted_value, float):
                    type_key = 'float'
                elif isinstance(casted_value, datetime):
                    type_key = 'datetime'
                else:
                    type_key = 'string'

                type_counts[type_key] = type_counts.get(type_key, 0) + 1

            # Find the most common type (excluding string)
            non_string_types = {k: v for k, v in type_counts.items() if k != 'string'}
            if not non_string_types:
                continue

            most_common_type = max(non_string_types, key=non_string_types.get)

            # Apply the cast based on most common type
            if most_common_type == 'bool':
                target_type = BooleanType()
                df = df.withColumn(field.name, col(field.name).cast(target_type))
            elif most_common_type == 'int':
                target_type = IntegerType()
                df = df.withColumn(field.name, col(field.name).cast(target_type))
            elif most_common_type == 'float':
                target_type = DoubleType()
                df = df.withColumn(field.name, col(field.name).cast(target_type))
            elif most_common_type == 'datetime':
                target_type = TimestampType()
                df = df.withColumn(field.name, to_timestamp(col(field.name)))
            else:
                continue

            cast_columns.append(f"{field.name}: {target_type} (from {len(field_values)} samples)")

        cast_columns_details = "\n\t".join(cast_columns) if cast_columns else "None"
        self.logger.info(f"🐟 cast columns:\n\t{cast_columns_details}")

        return df

    def _cast_to_python_type(self, value):
        if not isinstance(value, str):
            return value

        value = value.strip()

        if not value:
            return None

        # Boolean check
        if value.lower() in ("true", "false"):
            return value.lower() == "true"

        # Float check
        try:
            if value.count(".") == 1:
                parts = value.split(".")
                if len(parts) == 2:
                    # Check if it's a valid float format (allowing negative numbers)
                    left_part = parts[0]
                    right_part = parts[1]

                    # Left part can be digits or negative digits
                    left_valid = left_part.isdigit() or (
                        left_part.startswith("-") and left_part[1:].isdigit()
                    )
                    # Right part must be digits only
                    right_valid = right_part.isdigit()

                    if left_valid and right_valid:
                        return float(value)
        except ValueError:
            pass

        # Integer check
        try:
            if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
                return int(value)
        except ValueError:
            pass

        # DateTime check
        try:
            parsed_date = pd.to_datetime(value, errors="raise")
            return parsed_date.to_pydatetime()
        except (
            ValueError,
            TypeError,
            pd.errors.OutOfBoundsDatetime,
            pd.errors.ParserError,
        ):
            pass

        # Return String if no other type matches
        return value
