from databricks.sdk.runtime import *
import json
import os
import random
import re
import requests
from utility import (
    SchemaHealth,
    SchemaType,
    get_mockaroo_schema_name_metadata,
    init_logger,
    sleep_with_jitter,
)


class MockarooConnector:
    def __init__(self):
        self.logger = init_logger()

        # Load Mockaroo schema endpoint map
        with open("../data/mockaroo_schema_endpoint_map.json", "r") as f:
            self.schema_endpoint_map = json.load(f)

    def setup_widgets(self):
        # Remove old widgets
        dbutils.widgets.removeAll()

        dbutils.widgets.text("CATALOG_NAME", "roo_bricks", "Catalog Name")
        dbutils.widgets.text(
            "DATABRICKS_SCHEMA_NAME", "mockaroo_data", "Databricks Schema Name"
        )
        dbutils.widgets.text("PAGE_COUNT", "1", "Page Count (Generated Only)")
        dbutils.widgets.text("PAGE_SIZE", "1000", "Page Size")
        dbutils.widgets.dropdown(
            "RANDOM_FINAL_PAGE_SIZE",
            "true",
            ["true", "false"],
            "Random Final Page Size",
        )
        dbutils.widgets.text("VOLUME_NAME", "raw_data", "Volume Name")

        schema_names = list(self.schema_endpoint_map.keys())
        default_schema_name = schema_names[0]

        dbutils.widgets.dropdown(
            "MOCKAROO_SCHEMA_NAME",
            default_schema_name,
            schema_names,
            "!Mockaroo Schema Name",
        )

    def download(self):
        try:
            catalog_name = dbutils.widgets.get("CATALOG_NAME")
            databricks_schema_name = dbutils.widgets.get("DATABRICKS_SCHEMA_NAME")
            mockaroo_schema_name = dbutils.widgets.get("MOCKAROO_SCHEMA_NAME")
            page_count = int(dbutils.widgets.get("PAGE_COUNT"))
            page_size = int(dbutils.widgets.get("PAGE_SIZE"))
            random_final_page_size = bool(dbutils.widgets.get("RANDOM_FINAL_PAGE_SIZE"))
            volume_name = dbutils.widgets.get("VOLUME_NAME")

            (schema_health, schema_type, schema_name) = (
                get_mockaroo_schema_name_metadata(mockaroo_schema_name)
            )

            schema_display_name = f"{schema_health.value} {schema_name}"

            base_url = (
                "https://api.mockaroo.com/api"
                if schema_type == SchemaType.GENERATED
                else "https://my.api.mockaroo.com"
            )

            api_endpoint = self.schema_endpoint_map[mockaroo_schema_name]
            request_url = f"{base_url}/{api_endpoint}"
            self.logger.info(f"🔗 Schema request URL: {request_url}")

            api_params = {
                "count": page_size,
                "key": dbutils.secrets.get(scope="roo-bricks", key="mockaroo-api-key"),
            }

            self.logger.info(f"📇 Preparing the {catalog_name} catalog...")
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

            schema_path = f"{catalog_name}.{databricks_schema_name}"
            self.logger.info(f"📋 Preparing the {schema_path} schema...")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_path}")

            raw_volume_path = f"{schema_path}.{volume_name}"
            self.logger.info(f"🗄️ Preparing the {raw_volume_path} volume...")
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {raw_volume_path}")

            entity_name = f"{schema_type.value.lower()}_{schema_health.value.lower()}_{schema_name.lower()}"
            output_path = f"/Volumes/{catalog_name}/{databricks_schema_name}/{volume_name}/{entity_name}"

            if schema_type == SchemaType.GENERATED:
                record_count = self._extract_from_generated_data(
                    request_url,
                    api_params,
                    output_path,
                    page_count,
                    random_final_page_size,
                    schema_display_name,
                )
            else:
                record_count = self._extract_from_static_data(
                    request_url,
                    api_params,
                    output_path,
                    page_size,
                    schema_display_name,
                )

            if record_count > 0:
                self.logger.info(f"📊 Total record count: {record_count}")
                self.logger.info(f"💾 Data saved to: {output_path}/")
                self.logger.info(
                    f"✅ Successfully processed the {schema_display_name} dataset!"
                )
            else:
                self.logger.error("❌ Unexpected state: No records received")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Network error: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ JSON parsing error: {e}")
        except Exception as e:
            self.logger.error(f"❌ Unexpected error: {e}")

    def _extract_from_generated_data(
        self,
        request_url,
        api_params,
        output_path,
        page_count,
        random_final_page_size,
        schema_display_name,
    ):
        self.logger.info(
            f"⚙️ Generating {page_count} pages of the {schema_display_name} dataset..."
        )

        record_count = 0

        for page_index in range(page_count):
            last_call = page_index >= page_count - 1

            if random_final_page_size and last_call:
                api_params["count"] = random.randint(0, api_params["count"])

            self.logger.info(
                f"🐕 Fetching page {page_index + 1} of {page_count} containing {api_params['count']} records..."
            )

            record_count += self._fetch_and_save(
                request_url, api_params, output_path, page_index + 1
            )

            if not last_call:
                sleep_with_jitter(self.logger)

        return record_count

    def _extract_from_static_data(
        self, request_url, api_params, output_path, page_size, schema_display_name
    ):
        self.logger.info(
            f"⬇️ Downloading the {schema_display_name} dataset with {page_size} records per page..."
        )

        record_count = 0
        page_index = 0

        while True:
            self.logger.info(f"🐕 Fetching page {page_index + 1}...")

            api_params["limit"] = page_size
            api_params["offset"] = page_size * page_index

            page_record_count = self._fetch_and_save(
                request_url, api_params, output_path, page_index + 1
            )

            if page_record_count < page_size:
                record_count += page_record_count
                break
            else:
                record_count += page_record_count
                sleep_with_jitter(self.logger)

            page_index += 1

        return record_count

    def _fetch_and_save(self, request_url, params, base_output_path, page_number):
        try:
            response = requests.get(request_url, params=params)

            if response.status_code == 200:
                self.logger.info(
                    f"🟢 Successfully fetched page {page_number}! Status code: {response.status_code}"
                )

                page_data = response.json()
                if page_data:
                    os.makedirs(base_output_path, exist_ok=True)
                    page_output_path = (
                        f"{base_output_path}/page_{page_number:03d}.jsonl"
                    )

                    with open(page_output_path, "w") as f:
                        for record in page_data:
                            f.write(json.dumps(record) + "\n")

                    self.logger.info(
                        f"💾 Saved {len(page_data)} records from page {page_number} to {page_output_path}"
                    )
                    return len(page_data)
                else:
                    self.logger.warning(f"⚠️ Page {page_number} returned no records")
                    return 0
            else:
                self.logger.error(
                    f"❌ Page {page_number} request failed with status code: {response.status_code}"
                )
                self.logger.error(f"📃 Response: {response.text}")
                return None

        except Exception as e:
            self.logger.error(f"❌ Error during API call for page {page_number}: {e}")
            return None
