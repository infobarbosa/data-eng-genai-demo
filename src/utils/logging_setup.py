"""Configuracao padronizada de logging."""

import logging
import sys


class LoggingSetup:
    """Configura o logging padrao da aplicacao."""

    @staticmethod
    def configure(
        level: int = logging.INFO,
        fmt: str = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    ) -> logging.Logger:
        """Configura e retorna o logger raiz da aplicacao."""
        logger = logging.getLogger("pipeline")

        if logger.handlers:
            return logger

        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger
