from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

from src.core.exceptions import DataSourceNotFoundError


class DataIOManager:
    """Resolves logical data-source IDs to physical paths using a catalog
    (Strategy Pattern for read/write formats)."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: Dict[str, Any],
        output_config: Dict[str, Any],
        base_path: str = "",
    ) -> None:
        self._spark = spark
        self._catalog = catalog
        self._output_config = output_config
        self._base_path = base_path

    def _resolve_path(self, relative_path: str) -> str:
        if self._base_path:
            return f"{self._base_path}/{relative_path}"
        return relative_path

    def read(self, source_id: str) -> DataFrame:
        """Read a DataFrame by its logical catalog ID."""
        if source_id not in self._catalog:
            raise DataSourceNotFoundError(source_id)

        entry = self._catalog[source_id]
        fmt = entry["format"]
        path = self._resolve_path(entry["path"])
        options: Dict[str, str] = entry.get("options", {})

        reader = self._spark.read.format(fmt)
        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(path)

    def write(self, df: DataFrame, output_id: str) -> None:
        """Write a DataFrame to the configured output location."""
        if output_id not in self._output_config:
            raise DataSourceNotFoundError(output_id)

        entry = self._output_config[output_id]
        fmt = entry.get("format", "parquet")
        path = self._resolve_path(entry["path"])
        mode = entry.get("mode", "overwrite")

        df.write.format(fmt).mode(mode).save(path)
