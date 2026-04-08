"""Job de orquestracao do pipeline Top 10 Clientes."""

import logging

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms


class RunTop10Job:
    """Orquestra o pipeline que identifica os Top 10 clientes."""

    def __init__(
        self,
        config: ConfigLoader,
        data_io: DataIOManager,
        transforms: VendasTransforms,
    ) -> None:
        self._config = config
        self._data_io = data_io
        self._transforms = transforms
        self._logger = logging.getLogger("pipeline.run_top_10")

    def execute(self) -> None:
        """Executa o pipeline completo."""
        self._logger.info("Iniciando pipeline Top 10 Clientes...")

        # 1. Leitura dos dados
        self._logger.info("Etapa 1: Leitura dos dados de entrada.")
        df_pedidos = self._data_io.read("pedidos_bronze")
        df_clientes = self._data_io.read("clientes_bronze")

        self._logger.info(
            "Pedidos: %d registros | Clientes: %d registros",
            len(df_pedidos),
            len(df_clientes),
        )

        # 2. Transformacoes
        self._logger.info("Etapa 2: Aplicando transformacoes.")
        top_n = self._config.get_pipeline_param("top_n")
        df_top = self._transforms.top_n_clientes(df_pedidos, df_clientes, n=top_n)

        self._logger.info("Resultado:\n%s", df_top.to_string())

        # 3. Escrita do resultado
        self._logger.info("Etapa 3: Salvando resultado.")
        output_path = self._data_io.write(df_top, "top_10_clientes")

        self._logger.info(
            "Pipeline concluido com sucesso! Resultado salvo em: %s", output_path
        )
