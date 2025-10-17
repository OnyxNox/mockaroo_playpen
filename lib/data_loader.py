#!/usr/bin/env python3
import argparse
from dotenv import load_dotenv
import duckdb
import os
import re
import rbutils


class DataLoader:
    def __init__(self):
        """
        Initialize the DataLoader and prepare the environment for data loading operations.
        """
        load_dotenv()

        args = self._parse_args()

        run_id = rbutils.get_run_id(args.run_id)

        run_input_path = os.path.join(args.input, run_id)

        if not os.path.exists(run_input_path):
            raise FileNotFoundError(f"Path does not exist: {run_input_path}")

        entity_regex = re.compile(rf"^{re.escape(args.entity_name)}\.[^.]+$", re.IGNORECASE)

        run_input_files = os.listdir(run_input_path)
        entity_files = [
            os.path.join(run_input_path, run_input_file)
            for run_input_file in run_input_files
            if os.path.isfile(os.path.join(run_input_path, run_input_file)) and entity_regex.match(run_input_file)
        ]

        if not entity_files:
            raise FileNotFoundError(f"No files found matching entity name '{args.entity_name}' in {run_input_path}")

        self._entity_name = os.path.splitext(os.path.basename(entity_files[0]))[0]
        self._entity_path = entity_files[0]
        self._db_path = os.path.join(run_input_path, "roo_bricks.db")

        self._logger = rbutils.init_logger("DataLoader", args.log_level, run_input_path)

        self._logger.info("\n" + "=" * 120 + "\n")
        self._logger.debug(f"Arguments: {args}")
        self._logger.info(f"Run ID: {run_id}")
        self._logger.info(f"Entity Name: {self._entity_name}")
        self._logger.info(f"Entity Path: {self._entity_path}")
        self._logger.info(f"Database Path: {self._db_path}")

        if len(entity_files) > 1:
            self._logger.warning(
                f"Multiple files found matching entity name '{args.entity_name}': {entity_files}. Using first: {self._entity_path}"
            )

    def load(self):
        conn = duckdb.connect(self._db_path)

        entity_file_ext = os.path.splitext(self._entity_path)[1].lower()

        if entity_file_ext == ".csv":
            conn.execute(
                f"CREATE OR REPLACE TABLE {self._entity_name} AS SELECT * FROM read_csv_auto('{self._entity_path}');"
            )
        elif entity_file_ext == ".json":
            conn.execute(
                f"CREATE OR REPLACE TABLE {self._entity_name} AS SELECT * FROM read_json_auto('{self._entity_path}');"
            )
        else:
            raise ValueError(f"Unsupported file format: {entity_file_ext}")

        row_count = conn.execute(f"SELECT COUNT(*) FROM {self._entity_name};").fetchone()[0]
        self._logger.info(f"Created '{self._entity_name}' table with {row_count} rows")

        conn.close()

    def _parse_args(self):
        """
        Parse and return command-line arguments.

        Returns:
            argparse.Namespace: Parsed arguments.
        """
        parser = argparse.ArgumentParser(description="", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument(
            "--log-level",
            type=str.upper,
            default="INFO",
            choices=["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
            help="Logging level",
        )
        parser.add_argument("-i", "--input", default=".output", help="Downloaded data output path")
        parser.add_argument("--run-id", help="Run ID")
        parser.add_argument("entity_name", help="")

        return parser.parse_args()


if __name__ == "__main__":
    data_loader = DataLoader()
    data_loader.load()
