"""Logica pura de transformacao para o pipeline de vendas."""

import logging

import pandas as pd

from src.core.exceptions import TransformError


class VendasTransforms:
    """Transformacoes puras sobre DataFrames Pandas para analise de vendas."""

    def __init__(self) -> None:
        self._logger = logging.getLogger("pipeline.vendas_transforms")

    def calcular_valor_total(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula o valor total de cada pedido (VALOR_UNITARIO * QUANTIDADE).

        Args:
            df: DataFrame de pedidos com colunas VALOR_UNITARIO e QUANTIDADE.

        Returns:
            DataFrame com a coluna VALOR_TOTAL adicionada.
        """
        required_cols = {"VALOR_UNITARIO", "QUANTIDADE"}
        self._validate_columns(df, required_cols)

        result = df.copy()
        result["VALOR_TOTAL"] = result["VALOR_UNITARIO"] * result["QUANTIDADE"]
        self._logger.info("Coluna VALOR_TOTAL calculada para %d pedidos.", len(result))
        return result

    def agregar_por_cliente(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrega os pedidos por cliente, somando o valor total.

        Args:
            df: DataFrame de pedidos com colunas ID_CLIENTE e VALOR_TOTAL.

        Returns:
            DataFrame agrupado com ID_CLIENTE e VALOR_TOTAL_COMPRAS.
        """
        required_cols = {"ID_CLIENTE", "VALOR_TOTAL"}
        self._validate_columns(df, required_cols)

        result = df.groupby("ID_CLIENTE", as_index=False).agg(
            VALOR_TOTAL_COMPRAS=("VALOR_TOTAL", "sum")
        )
        self._logger.info("Agregacao concluida: %d clientes unicos.", len(result))
        return result

    def top_n_clientes(
        self,
        df_pedidos: pd.DataFrame,
        df_clientes: pd.DataFrame,
        n: int = 10,
    ) -> pd.DataFrame:
        """Retorna os Top N clientes por volume total de compras.

        Aplica o pipeline completo: calcula valor total, agrega por cliente,
        faz join com dados de clientes e retorna os top N.

        Args:
            df_pedidos: DataFrame de pedidos.
            df_clientes: DataFrame de clientes com colunas id e nome.
            n: Numero de top clientes a retornar.

        Returns:
            DataFrame com o ranking dos top N clientes.
        """
        df_com_total = self.calcular_valor_total(df_pedidos)
        df_agregado = self.agregar_por_cliente(df_com_total)

        df_ranking = pd.merge(
            df_agregado,
            df_clientes[["id", "nome"]],
            left_on="ID_CLIENTE",
            right_on="id",
            how="inner",
        )

        df_ranking = df_ranking.drop(columns=["id"])
        df_ranking = df_ranking.sort_values(
            "VALOR_TOTAL_COMPRAS", ascending=False
        ).head(n)
        df_ranking = df_ranking.reset_index(drop=True)
        df_ranking.index = df_ranking.index + 1
        df_ranking.index.name = "RANKING"

        self._logger.info("Top %d clientes calculados com sucesso.", n)
        return df_ranking

    @staticmethod
    def _validate_columns(df: pd.DataFrame, required: set) -> None:
        """Valida se o DataFrame possui as colunas necessarias."""
        missing = required - set(df.columns)
        if missing:
            raise TransformError(
                f"Colunas obrigatorias ausentes no DataFrame: {missing}"
            )
