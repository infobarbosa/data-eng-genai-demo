import logging
import sys


class LoggingSetup:
    """Configures application-wide logging."""

    @staticmethod
    def configure(level: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger("top10_pipeline")
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(level)
        return logger
