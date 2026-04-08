"""Ponto de entrada da aplicacao - Composition Root."""

import logging
import os
import subprocess
import sys

from src.core.config import ConfigLoader
from src.core.exceptions import ConfigLoadError, DataIOError, TransformError
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.transforms.vendas_transforms import VendasTransforms
from src.utils.logging_setup import LoggingSetup
from src.utils.pandas_manager import PandasManager


def download_datasets(config: ConfigLoader, logger: logging.Logger) -> None:
    """Baixa os datasets de exemplo caso nao existam localmente."""
    input_base = config.get_input_base_path()
    os.makedirs(input_base, exist_ok=True)

    datasets_config = config.config.get("datasets", {})
    for name, info in datasets_config.items():
        local_path = input_base / info["local_path"]
        if local_path.exists():
            logger.info("Dataset '%s' ja existe em: %s", name, local_path)
            continue

        repo_url = info["repo_url"]
        logger.info("Baixando dataset '%s' de: %s", name, repo_url)
        try:
            subprocess.run(
                ["git", "clone", repo_url, str(local_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            logger.info("Dataset '%s' baixado com sucesso.", name)
        except subprocess.CalledProcessError as e:
            logger.error("Erro ao baixar dataset '%s': %s", name, e.stderr)
            raise DataIOError(f"Falha ao baixar dataset '{name}': {e.stderr}") from e


def main() -> None:
    """Funcao principal - Composition Root."""
    # 1. Configuracao de logging
    logger = LoggingSetup.configure()
    logger.info("Iniciando aplicacao...")

    try:
        # 2. Carregamento da configuracao
        config = ConfigLoader()
        logger.info("Configuracao carregada: %s", config.config["project"]["name"])

        # 3. Inicializacao do PandasManager
        pandas_mgr = PandasManager()
        logger.info("Pandas versao: %s", pandas_mgr.get_version())

        # 4. Download dos datasets
        download_datasets(config, logger)

        # 5. Inicializacao do DataIOManager
        data_io = DataIOManager(config)

        # 6. Inicializacao das transformacoes
        transforms = VendasTransforms()

        # 7. Execucao do pipeline
        job = RunTop10Job(
            config=config,
            data_io=data_io,
            transforms=transforms,
        )
        job.execute()

        logger.info("Aplicacao finalizada com sucesso!")

    except (ConfigLoadError, DataIOError, TransformError) as e:
        logger.error("Erro no pipeline: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Erro inesperado: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
