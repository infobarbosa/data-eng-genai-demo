from typing import Any, Dict

from pyspark.sql import SparkSession


class SparkManager:
    """Factory that creates and manages a SparkSession."""

    def __init__(self, spark_config: Dict[str, Any]) -> None:
        self._app_name: str = spark_config.get("app_name", "DefaultSparkApp")
        self._master: str = spark_config.get("master", "local[*]")
        self._session: SparkSession | None = None

    def get_or_create(self) -> SparkSession:
        if self._session is None:
            self._session = (
                SparkSession.builder.appName(self._app_name)
                .master(self._master)
                .getOrCreate()
            )
        return self._session

    def stop(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None
