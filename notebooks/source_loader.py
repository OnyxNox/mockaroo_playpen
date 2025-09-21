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
        last_row = df.tail(1)[0] if df.count() > 0 else None

        if not last_row:
            self.logger.warning("DataFrame is empty, returning original DataFrame")
            return df

        # Build list of columns to cast
        cast_columns = []

        for field in df.schema.fields:
            if field.dataType != StringType():
                continue

            field_value = last_row[field.name]

            if field_value is not None:
                casted_value = self._cast_to_python_type(field_value)

                if isinstance(casted_value, bool):
                    target_type = BooleanType()
                elif isinstance(casted_value, int):
                    target_type = IntegerType()
                elif isinstance(casted_value, float):
                    target_type = DoubleType()
                elif isinstance(casted_value, datetime):
                    target_type = TimestampType()
                    df = df.withColumn(field.name, to_timestamp(col(field.name)))
                    cast_columns.append(f"{field.name}: {target_type}")
                    continue
                else:
                    continue

                df = df.withColumn(field.name, col(field.name).cast(target_type))
                cast_columns.append(f"{field.name}: {target_type}")

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
