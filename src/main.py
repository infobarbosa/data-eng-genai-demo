"""Ponto de entrada da aplicacao - Composition Root."""

import os

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.utils.logging_setup import LoggingSetup
from src.utils.spark_manager import SparkManager


def main() -> None:
    """Composition Root: instancia e injeta todas as dependencias, depois executa o pipeline."""
    # Configurar logging
    logger = LoggingSetup.configure()
    logger.info("Inicializando o pipeline...")

    # Determinar o diretorio raiz do projeto
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root_dir, "config", "config.yaml")

    # Carregar configuracao
    logger.info("Carregando configuracao de: %s", config_path)
    config = ConfigLoader(config_path)

    # Criar SparkManager e obter a sessao
    spark_manager = SparkManager(config.spark_config)
    spark = spark_manager.get_or_create()

    try:
        # Criar DataIOManager com injecao de dependencia
        data_io = DataIOManager(
            spark=spark,
            catalogo=config.catalogo,
            base_path=root_dir,
        )

        # Criar e executar o job
        top_n = config.pipeline_config.get("top_n", 10)
        job = RunTop10Job(data_io=data_io, top_n=top_n)
        job.executar()

    finally:
        # Garantir que a sessao seja encerrada
        spark_manager.stop()

    logger.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()
