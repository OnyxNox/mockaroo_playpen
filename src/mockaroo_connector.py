#!/usr/bin/env python3
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv
import glob
import json
import logging
import os
import requests
import time
import uuid


class MockarooConnector:
    def __init__(self):
        """
        Initialize the MockarooConnector with configuration and runtime settings.
        """
        load_dotenv()

        args = self._parse_args()

        run_id = (
            args.run_id
            or os.getenv("ROO_DATA_RUN_ID")
            or f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}"
        )

        self._run_output_path = os.path.join(args.output, run_id)
        os.makedirs(self._run_output_path, exist_ok=True)

        self._schema_files = self._get_schema_files(args.mockaroo_schemas)
        self._concurrency = args.concurrency

        logging.basicConfig(
            level=getattr(logging, args.log_level),
            format="[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(os.path.join(self._run_output_path, "roo_data.log")),
            ],
        )

        self._logger = logging.getLogger(__name__)

        self._logger.info("\n" + "=" * 120 + "\n")
        self._logger.debug(f"Arguments: {args}")
        self._logger.info(f"Run ID: {run_id}")
        self._logger.info(f"Schema Count: {len(self._schema_files)}")
        self._logger.info(f"Concurrency: {self._concurrency}")

        self._mockaroo_api_key = os.getenv("MOCKAROO_API_KEY")
        if not self._mockaroo_api_key:
            raise ValueError("MOCKAROO_API_KEY environment variable not set")

    def download(self):
        """
        Retrieve mock data from the Mockaroo service and save to the configured output location.
        Downloads schemas in parallel with configurable concurrency limit.
        """
        with ThreadPoolExecutor(max_workers=self._concurrency) as executor:
            future_to_schema = {
                executor.submit(self._download_schema, schema_file): schema_file for schema_file in self._schema_files
            }

            completed = 0
            failed = 0
            for future in as_completed(future_to_schema):
                schema_file = future_to_schema[future]
                try:
                    future.result()
                    completed += 1

                    self._logger.info(f"Progress: {completed + failed}/{len(self._schema_files)} datasets downloaded")
                except Exception as e:
                    failed += 1

                    self._logger.error(f"Failed to process schema {schema_file}: {e}")

        self._logger.info(f"Completed: {completed} successful, {failed} failed")

    def _download_schema(self, schema_file):
        """
        Download data for a single schema file.

        Args:
            schema_file (str): Path to the schema file to process.
        """
        with open(schema_file, "r") as mockaroo_schema_file:
            mockaroo_schema = json.loads(mockaroo_schema_file.read())

        schema_name = mockaroo_schema.get("name", "mockaroo_data")
        count = mockaroo_schema.get("num_rows", 1)
        file_format = mockaroo_schema.get("file_format", "csv")
        line_ending = mockaroo_schema.get("line_ending", "unix")
        schema = mockaroo_schema.get("columns", [])

        output_filename = f"{schema_name}.{file_format}"
        output_filepath = os.path.join(self._run_output_path, output_filename)

        generate_url = f"https://api.mockaroo.com/api/generate.{file_format}"
        generate_url += f"?count={count}&line_ending={line_ending}"

        headers = {"Content-Type": "application/json", "X-API-Key": self._mockaroo_api_key}

        self._logger.info(f"[{schema_name}] Calling POST {generate_url}")

        start_time = time.time()

        response = requests.post(generate_url, json=schema, headers=headers)
        response.raise_for_status()

        duration = time.time() - start_time

        self._logger.info(
            f"[{schema_name}] POST {generate_url} - HTTP {response.status_code} ({response.reason}) - {duration:.2f}s"
        )

        with open(output_filepath, "w") as output_file:
            output_file.write(response.text)

        self._logger.info(f"[{schema_name}] Saved to {output_filepath}")

    def _get_schema_files(self, path):
        """
        Get list of schema files from a file path or directory.

        Args:
            path (str): Path to a schema file or directory containing schema files.

        Returns:
            list: List of schema file paths.
        """
        if os.path.isfile(path):
            return [path]
        elif os.path.isdir(path):
            json_files = glob.glob(os.path.join(path, "*.json"))
            if not json_files:
                raise ValueError(f"No JSON schema files found in directory: {path}")

            return json_files
        else:
            raise ValueError(f"Path does not exist: {path}")

    def _parse_args(self):
        """
        Process command-line interface inputs for the connector.

        Returns:
            argparse.Namespace: Parsed command-line arguments.
        """
        parser = argparse.ArgumentParser(
            description="Download data from Mockaroo API", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

        parser.add_argument(
            "--concurrency",
            type=int,
            default=5,
            help="Maximum number of concurrent API calls when processing multiple schemas",
        )
        parser.add_argument(
            "--log-level",
            type=str.upper,
            default="INFO",
            choices=["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
            help="Logging level",
        )
        parser.add_argument("-o", "--output", default=".output", help="Downloaded data output path")
        parser.add_argument("--run-id", help="Run ID to use instead of auto-generating one")
        parser.add_argument("mockaroo_schemas", help="Mockaroo schema file path or directory containing schema files")

        return parser.parse_args()


if __name__ == "__main__":
    mockaroo_connector = MockarooConnector()
    mockaroo_connector.download()
