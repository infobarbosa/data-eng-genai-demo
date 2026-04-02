"""Modulo de orquestracao do pipeline Top 10 Clientes (sem frameworks)."""

import logging
from typing import Any, Dict, List

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
        pedidos = self._data_io.ler("pedidos_bronze")
        clientes = self._data_io.ler("clientes_bronze")

        # 2. Calcular valor total por pedido
        self._logger.info("Etapa 2: Calculando valor total por pedido.")
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos)

        # 3. Agregar por cliente
        self._logger.info("Etapa 3: Agregando valores por cliente.")
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)

        # 4. Rankear top N
        self._logger.info("Etapa 4: Rankeando top %d clientes.", self._top_n)
        ranking = VendasTransforms.rankear_top_n(agregado, self._top_n)

        # 5. Enriquecer com dados de clientes
        self._logger.info("Etapa 5: Enriquecendo ranking com dados de clientes.")
        resultado = VendasTransforms.enriquecer_com_clientes(ranking, clientes)

        # 6. Exibir resultado
        self._logger.info("Resultado final - Top %d Clientes:", self._top_n)
        self._exibir_resultado(resultado)

        # 7. Salvar resultado
        self._logger.info("Etapa 6: Salvando resultado.")
        self._data_io.escrever(resultado, "top_10_clientes")

        self._logger.info("Pipeline Top %d Clientes concluido com sucesso!", self._top_n)

    def _exibir_resultado(self, dados: List[Dict[str, Any]]) -> None:
        """Exibe o resultado no formato tabular no log."""
        if not dados:
            self._logger.info("Nenhum resultado para exibir.")
            return

        colunas = list(dados[0].keys())
        header = " | ".join(f"{col:>15}" for col in colunas)
        separador = "-" * len(header)

        self._logger.info(separador)
        self._logger.info(header)
        self._logger.info(separador)
        for registro in dados:
            linha = " | ".join(f"{str(registro.get(col, '')):>15}" for col in colunas)
            self._logger.info(linha)
        self._logger.info(separador)
