"""PandasManager - factory e configuracao global do Pandas."""

import logging

import pandas as pd


class PandasManager:
    """Gerencia configuracoes globais do Pandas."""

    def __init__(self) -> None:
        self._logger = logging.getLogger("pipeline.pandas_manager")
        self._configure()

    def _configure(self) -> None:
        """Aplica configuracoes globais do Pandas."""
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", 100)
        pd.set_option("display.width", None)
        pd.set_option("display.float_format", "{:.2f}".format)
        self._logger.info("PandasManager configurado com sucesso.")

    @staticmethod
    def get_version() -> str:
        """Retorna a versao do Pandas."""
        return pd.__version__

    def reset(self) -> None:
        """Reseta as configuracoes do Pandas para o padrao."""
        pd.reset_option("all")
        self._logger.info("Configuracoes do Pandas resetadas.")
