import logging

from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms


class RunTop10Job:
    """Orchestrates the Top-10 Customers pipeline."""

    def __init__(
        self,
        data_io: DataIOManager,
        logger: logging.Logger | None = None,
    ) -> None:
        self._data_io = data_io
        self._logger = logger or logging.getLogger("top10_pipeline")

    def execute(self) -> None:
        self._logger.info("Starting Top-10 Customers pipeline...")

        self._logger.info("Reading 'pedidos' data source...")
        pedidos = self._data_io.read("pedidos")

        self._logger.info("Reading 'clientes' data source...")
        clientes = self._data_io.read("clientes")

        self._logger.info("Computing Top-10 customers by purchase volume...")
        top_10 = VendasTransforms.top_10_clientes(pedidos, clientes)

        self._logger.info("Writing results to 'top_10_clientes' output...")
        self._data_io.write(top_10, "top_10_clientes")

        self._logger.info("Pipeline completed successfully.")
