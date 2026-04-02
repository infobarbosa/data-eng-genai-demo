"""Modulo responsavel pela leitura e escrita de dados via catalogo."""

import logging
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

from src.core.exceptions import DataIOError


class DataIOManager:
    """Gerencia operacoes de leitura e escrita de dados utilizando o catalogo de configuracao."""

    def __init__(self, spark: SparkSession, catalogo: Dict[str, Dict[str, Any]], base_path: str) -> None:
        self._spark = spark
        self._catalogo = catalogo
        self._base_path = base_path
        self._logger = logging.getLogger("top10_pipeline.DataIOManager")

    def _resolver_caminho(self, caminho_relativo: str) -> str:
        """Resolve o caminho fisico a partir do caminho relativo no catalogo."""
        import os

        return os.path.join(self._base_path, caminho_relativo)

    def ler(self, dataset_id: str) -> DataFrame:
        """Le um dataset pelo seu ID logico no catalogo."""
        if dataset_id not in self._catalogo:
            raise DataIOError(f"Dataset '{dataset_id}' nao encontrado no catalogo.")

        dataset_config = self._catalogo[dataset_id]
        caminho = self._resolver_caminho(dataset_config["caminho"])
        formato = dataset_config["formato"]
        opcoes = dataset_config.get("opcoes", {})

        self._logger.info("Lendo dataset '%s' de '%s' (formato: %s)", dataset_id, caminho, formato)

        try:
            reader = self._spark.read.format(formato)
            for chave, valor in opcoes.items():
                reader = reader.option(chave, valor)
            df = reader.load(caminho)
            self._logger.info("Dataset '%s' carregado com %d registros.", dataset_id, df.count())
            return df
        except Exception as e:
            raise DataIOError(f"Erro ao ler dataset '{dataset_id}' de '{caminho}': {e}")

    def escrever(self, df: DataFrame, dataset_id: str) -> None:
        """Escreve um DataFrame no destino definido pelo ID logico no catalogo."""
        if dataset_id not in self._catalogo:
            raise DataIOError(f"Dataset '{dataset_id}' nao encontrado no catalogo.")

        dataset_config = self._catalogo[dataset_id]
        caminho = self._resolver_caminho(dataset_config["caminho"])
        formato = dataset_config["formato"]
        modo = dataset_config.get("modo", "overwrite")

        self._logger.info("Escrevendo dataset '%s' em '%s' (formato: %s, modo: %s)", dataset_id, caminho, formato, modo)

        try:
            df.write.format(formato).mode(modo).save(caminho)
            self._logger.info("Dataset '%s' salvo com sucesso.", dataset_id)
        except Exception as e:
            raise DataIOError(f"Erro ao escrever dataset '{dataset_id}' em '{caminho}': {e}")
