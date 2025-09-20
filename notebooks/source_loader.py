from databricks.sdk.runtime import dbutils, spark
from delta.tables import DeltaTable
import json
from pyspark.sql.functions import current_timestamp, lit, col
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
            source_path = f"/Volumes/{catalog_name}/{databricks_schema_name}/{volume_name}/{entity_name}"
            target_table_name = (
                f"{catalog_name}.{databricks_schema_name}.src_{entity_name}"
            )

            self.logger.info(f"📁 Source: {source_path}")
            self.logger.info(f"🎯 Target: {target_table_name}")

            df = spark.read.json(f"{source_path}/*.jsonl")

            df = df.withColumn(
                "__m_ingestion_timestamp", current_timestamp()
            ).withColumn("__m_source_file", col("_metadata.file_path"))

            self.logger.info(f"🔄 Refreshing target table with {df.count()} records...")
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(target_table_name)

            self.logger.info(
                f"✅ Successfully processed {schema_display_name} dataset!"
            )

        except Exception as e:
            self.logger.error(f"❌ Unexpected error: {e}")
