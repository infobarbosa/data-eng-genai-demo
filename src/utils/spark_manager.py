"""Modulo responsavel pela criacao e gerenciamento da SparkSession."""

import logging
from typing import Dict

from pyspark.sql import SparkSession


class SparkManager:
    """Factory para criar e gerenciar a SparkSession."""

    def __init__(self, spark_config: Dict[str, str]) -> None:
        self._app_name = spark_config.get("app_name", "SparkApp")
        self._master = spark_config.get("master", "local[*]")
        self._session: SparkSession = None
        self._logger = logging.getLogger("top10_pipeline.SparkManager")

    def get_or_create(self) -> SparkSession:
        """Retorna a SparkSession existente ou cria uma nova."""
        if self._session is None:
            self._logger.info("Criando SparkSession: app_name=%s, master=%s", self._app_name, self._master)
            self._session = SparkSession.builder.appName(self._app_name).master(self._master).getOrCreate()
            self._session.sparkContext.setLogLevel("WARN")
            self._logger.info("SparkSession criada com sucesso.")
        return self._session

    def stop(self) -> None:
        """Encerra a SparkSession."""
        if self._session is not None:
            self._logger.info("Encerrando SparkSession.")
            self._session.stop()
            self._session = None
