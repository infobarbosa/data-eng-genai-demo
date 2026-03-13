"""Composition Root – instantiates and injects all dependencies."""

import os
import subprocess
import sys

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.utils.logging_setup import LoggingSetup
from src.utils.spark_manager import SparkManager


def _ensure_datasets(base_path: str) -> None:
    """Clone example datasets if they are not already present."""
    datasets = [
        {
            "repo": "https://github.com/infobarbosa/dataset-json-clientes",
            "dest": os.path.join(
                base_path, "data", "input", "dataset-json-clientes"
            ),
        },
        {
            "repo": "https://github.com/infobarbosa/datasets-csv-pedidos",
            "dest": os.path.join(
                base_path, "data", "input", "datasets-csv-pedidos"
            ),
        },
    ]
    for ds in datasets:
        if not os.path.isdir(ds["dest"]):
            print(f"Cloning {ds['repo']} -> {ds['dest']}")
            subprocess.run(
                ["git", "clone", ds["repo"], ds["dest"]],
                check=True,
            )


def main() -> None:
    # Determine project root (where config/ lives)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # --- Logging ---
    logger = LoggingSetup.configure()

    # --- Configuration ---
    config_path = os.path.join(project_root, "config", "config.yaml")
    config = ConfigLoader(config_path)

    # --- Download datasets ---
    _ensure_datasets(project_root)

    # --- Spark ---
    spark_manager = SparkManager(config.spark_config)
    spark = spark_manager.get_or_create()

    try:
        # --- Data I/O ---
        data_io = DataIOManager(
            spark=spark,
            catalog=config.catalog,
            output_config=config.output_config,
            base_path=project_root,
        )

        # --- Job ---
        job = RunTop10Job(data_io=data_io, logger=logger)
        job.execute()
    finally:
        spark_manager.stop()


if __name__ == "__main__":
    sys.exit(main() or 0)
