"""Modulo responsavel por carregar e validar a configuracao do pipeline."""

import os
from typing import Any, Dict

import yaml

from src.core.exceptions import ConfigError


class ConfigLoader:
    """Carrega e fornece acesso a configuracao do pipeline a partir de um arquivo YAML."""

    def __init__(self, config_path: str) -> None:
        self._config_path = config_path
        self._config: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Carrega o arquivo YAML de configuracao."""
        if not os.path.exists(self._config_path):
            raise ConfigError(f"Arquivo de configuracao nao encontrado: {self._config_path}")

        try:
            with open(self._config_path, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigError(f"Erro ao parsear o arquivo YAML: {e}")

        if not self._config:
            raise ConfigError("Arquivo de configuracao esta vazio.")

    @property
    def spark_config(self) -> Dict[str, str]:
        """Retorna a configuracao do Spark."""
        spark = self._config.get("spark")
        if not spark:
            raise ConfigError("Secao 'spark' nao encontrada na configuracao.")
        return spark

    @property
    def catalogo(self) -> Dict[str, Dict[str, Any]]:
        """Retorna o catalogo de dados."""
        catalogo = self._config.get("catalogo")
        if not catalogo:
            raise ConfigError("Secao 'catalogo' nao encontrada na configuracao.")
        return catalogo

    @property
    def pipeline_config(self) -> Dict[str, Any]:
        """Retorna a configuracao do pipeline."""
        pipeline = self._config.get("pipeline")
        if not pipeline:
            raise ConfigError("Secao 'pipeline' nao encontrada na configuracao.")
        return pipeline

    def get_dataset_config(self, dataset_id: str) -> Dict[str, Any]:
        """Retorna a configuracao de um dataset especifico pelo seu ID logico."""
        catalogo = self.catalogo
        if dataset_id not in catalogo:
            raise ConfigError(f"Dataset '{dataset_id}' nao encontrado no catalogo.")
        return catalogo[dataset_id]
