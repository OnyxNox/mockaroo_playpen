#!/usr/bin/env python3
import argparse
from datetime import datetime
from dotenv import load_dotenv
import json
import logging
import os
import requests
import time
import uuid


class MockarooConnector:
    def __init__(self):
        """
        Initialize the MockarooConnector.

        Sets up the connector by loading configuration, preparing the execution environment, and initializing logging.
        """
        load_dotenv()

        args = self._parse_args()

        run_id = (
            args.run_id
            or os.getenv("ROO_DATA_RUN_ID")
            or f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}"
        )

        run_output_path = os.path.join(args.output, run_id)
        os.makedirs(run_output_path, exist_ok=True)

        with open(args.mockaroo_schema, "r") as mockaroo_schema_file:
            mockaroo_schema = json.loads(mockaroo_schema_file.read())

        self._count: int = args.count or mockaroo_schema.get("num_rows", 1)
        self._file_format: str = mockaroo_schema.get("file_format", "csv")
        self._line_ending = mockaroo_schema.get("line_ending", "unix")
        self._schema = mockaroo_schema.get("columns", [])

        output_filename = f"{mockaroo_schema.get('name', 'mockaroo_data')}.{self._file_format}"
        self._output_filepath = os.path.join(run_output_path, output_filename)

        logging.basicConfig(
            level=getattr(logging, log_level),
            format="[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
            handlers=[logging.StreamHandler(), logging.FileHandler(os.path.join(run_output_path, "roo_data.log"))],
        )

        self._logger = logging.getLogger(__name__)

        self._logger.info("\n" + "=" * 120 + "\n")
        self._logger.debug(f"Arguments: {args}")
        self._logger.info(f"Run ID: {run_id}")
        self._logger.info(f"Mockaroo Schema File: {args.mockaroo_schema}")
        self._logger.info(f"Schema Column Count: {len(self._schema)}")
        self._logger.info(f"Line Ending: {self._line_ending}")
        self._logger.info(f"Output Filepath: {self._output_filepath}")
        self._logger.info(f"Record Count: {self._count}")

    def download(self):
        """
        Generate and download mock data from Mockaroo API.
        """
        generate_url = f"https://api.mockaroo.com/api/generate.{self._file_format}"
        generate_url += f"?count={self._count}&line_ending={self._line_ending}"

        mockaroo_api_key = os.getenv("MOCKAROO_API_KEY")
        if not mockaroo_api_key:
            raise ValueError("MOCKAROO_API_KEY environment variable not set")

        headers = {"Content-Type": "application/json", "X-API-Key": mockaroo_api_key}

        self._logger.info(f"Calling POST {generate_url}")

        start_time = time.time()

        response = requests.post(generate_url, json=self._schema, headers=headers)
        response.raise_for_status()

        duration = time.time() - start_time

        self._logger.info(f"POST {generate_url} - HTTP {response.status_code} ({response.reason}) - {duration:.2f}s")

        with open(self._output_filepath, "w") as output_file:
            output_file.write(response.text)

    def _parse_args(self):
        """
        Parse and return command-line arguments.

        Returns:
            argparse.Namespace: Parsed arguments.
        """
        parser = argparse.ArgumentParser(
            description="Download data from Mockaroo API", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

        parser.add_argument("-c", "--count", help="Number of rows to generate")
        parser.add_argument(
            "--log-level",
            type=str.upper,
            default="INFO",
            choices=["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
            help="Logging level",
        )
        parser.add_argument("-o", "--output", default=".output", help="Downloaded data output path")
        parser.add_argument("--run-id", help="Run ID to use instead of auto-generating one")
        parser.add_argument("mockaroo_schema", help="Mockaroo schema file path")

        return parser.parse_args()


if __name__ == "__main__":
    mockaroo_connector = MockarooConnector()
    mockaroo_connector.download()
