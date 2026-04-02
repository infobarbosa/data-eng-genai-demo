"""Modulo com logica pura de transformacao para o ranking de clientes."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class VendasTransforms:
    """Transformacoes puras sobre DataFrames de vendas/pedidos."""

    @staticmethod
    def calcular_valor_total(pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna VALOR_TOTAL = VALOR_UNITARIO * QUANTIDADE."""
        return pedidos_df.withColumn(
            "VALOR_TOTAL",
            F.col("VALOR_UNITARIO").cast("double") * F.col("QUANTIDADE").cast("int"),
        )

    @staticmethod
    def agregar_por_cliente(pedidos_df: DataFrame) -> DataFrame:
        """Agrupa por ID_CLIENTE e soma o VALOR_TOTAL."""
        return pedidos_df.groupBy("ID_CLIENTE").agg(
            F.sum("VALOR_TOTAL").alias("TOTAL_COMPRAS"),
            F.count("ID_PEDIDO").alias("QTD_PEDIDOS"),
        )

    @staticmethod
    def rankear_top_n(agregado_df: DataFrame, n: int = 10) -> DataFrame:
        """Ordena por TOTAL_COMPRAS decrescente e retorna os top N clientes."""
        window = Window.orderBy(F.col("TOTAL_COMPRAS").desc())
        return (
            agregado_df.withColumn("RANKING", F.row_number().over(window))
            .filter(F.col("RANKING") <= n)
            .orderBy("RANKING")
        )

    @staticmethod
    def enriquecer_com_clientes(ranking_df: DataFrame, clientes_df: DataFrame) -> DataFrame:
        """Faz join do ranking com dados de clientes para enriquecer o resultado."""
        return ranking_df.join(
            clientes_df,
            ranking_df["ID_CLIENTE"] == clientes_df["id"],
            "left",
        ).select(
            ranking_df["RANKING"],
            ranking_df["ID_CLIENTE"],
            clientes_df["nome"].alias("NOME_CLIENTE"),
            clientes_df["email"].alias("EMAIL"),
            ranking_df["TOTAL_COMPRAS"],
            ranking_df["QTD_PEDIDOS"],
        )
