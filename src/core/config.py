import os
from typing import Any, Dict

import yaml

from src.core.exceptions import ConfigNotFoundError


class ConfigLoader:
    """Loads and provides access to YAML configuration."""

    def __init__(self, config_path: str | None = None) -> None:
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..",
                "..",
                "config",
                "config.yaml",
            )
        self._config_path = os.path.normpath(config_path)
        self._config: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        if not os.path.isfile(self._config_path):
            raise ConfigNotFoundError(self._config_path)
        with open(self._config_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, dict):
            raise ValueError(f"Expected a YAML mapping in {self._config_path}")
        return data

    @property
    def spark_config(self) -> Dict[str, Any]:
        return self._config.get("spark", {})

    @property
    def catalog(self) -> Dict[str, Any]:
        return self._config.get("catalog", {})

    @property
    def output_config(self) -> Dict[str, Any]:
        return self._config.get("output", {})

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)
