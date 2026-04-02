"""Modulo de orquestracao do pipeline Top 10 Clientes."""

import logging

from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms


class RunTop10Job:
    """Orquestra o pipeline para identificar os Top 10 Clientes por volume de compras."""

    def __init__(self, data_io: DataIOManager, top_n: int = 10) -> None:
        self._data_io = data_io
        self._top_n = top_n
        self._logger = logging.getLogger("top10_pipeline.RunTop10Job")

    def executar(self) -> None:
        """Executa o pipeline completo."""
        self._logger.info("Iniciando pipeline Top %d Clientes...", self._top_n)

        # 1. Leitura dos dados
        self._logger.info("Etapa 1: Leitura dos dados de pedidos e clientes.")
        pedidos_df = self._data_io.ler("pedidos_bronze")
        clientes_df = self._data_io.ler("clientes_bronze")

        # 2. Calcular valor total por pedido
        self._logger.info("Etapa 2: Calculando valor total por pedido.")
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)

        # 3. Agregar por cliente
        self._logger.info("Etapa 3: Agregando valores por cliente.")
        agregado_df = VendasTransforms.agregar_por_cliente(pedidos_com_total)

        # 4. Rankear top N
        self._logger.info("Etapa 4: Rankeando top %d clientes.", self._top_n)
        ranking_df = VendasTransforms.rankear_top_n(agregado_df, self._top_n)

        # 5. Enriquecer com dados de clientes
        self._logger.info("Etapa 5: Enriquecendo ranking com dados de clientes.")
        resultado_df = VendasTransforms.enriquecer_com_clientes(ranking_df, clientes_df)

        # 6. Exibir resultado
        self._logger.info("Resultado final - Top %d Clientes:", self._top_n)
        resultado_df.show(truncate=False)

        # 7. Salvar resultado
        self._logger.info("Etapa 6: Salvando resultado.")
        self._data_io.escrever(resultado_df, "top_10_clientes")

        self._logger.info("Pipeline Top %d Clientes concluido com sucesso!", self._top_n)
