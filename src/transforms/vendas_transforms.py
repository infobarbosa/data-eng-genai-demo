from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class VendasTransforms:
    """Pure transformation logic for sales data.

    All methods receive DataFrames and return DataFrames,
    keeping the transformation layer free of any I/O side effects.
    """

    @staticmethod
    def calcular_valor_total_pedido(pedidos: DataFrame) -> DataFrame:
        """Add a column with total order value (unit price * quantity)."""
        return pedidos.withColumn(
            "VALOR_TOTAL",
            F.col("VALOR_UNITARIO").cast("double")
            * F.col("QUANTIDADE").cast("int"),
        )

    @staticmethod
    def agregar_por_cliente(pedidos_com_total: DataFrame) -> DataFrame:
        """Aggregate total purchase volume per customer."""
        return pedidos_com_total.groupBy("ID_CLIENTE").agg(
            F.sum("VALOR_TOTAL").alias("TOTAL_COMPRAS"),
            F.count("ID_PEDIDO").alias("QTD_PEDIDOS"),
        )

    @staticmethod
    def rankear_top_n(agregado: DataFrame, n: int = 10) -> DataFrame:
        """Rank customers by total purchase volume and return the top N."""
        window = Window.orderBy(F.col("TOTAL_COMPRAS").desc())
        return (
            agregado.withColumn("RANKING", F.row_number().over(window))
            .filter(F.col("RANKING") <= n)
            .orderBy("RANKING")
        )

    @staticmethod
    def enriquecer_com_clientes(
        ranking: DataFrame, clientes: DataFrame
    ) -> DataFrame:
        """Join ranking with customer details."""
        clientes_sel = clientes.select(
            F.col("id").alias("ID_CLIENTE_REF"),
            F.col("nome"),
            F.col("email"),
        )
        return ranking.join(
            clientes_sel,
            ranking["ID_CLIENTE"] == clientes_sel["ID_CLIENTE_REF"],
            "left",
        ).drop("ID_CLIENTE_REF")

    @classmethod
    def top_10_clientes(
        cls, pedidos: DataFrame, clientes: DataFrame
    ) -> DataFrame:
        """Full pipeline: compute top-10 customers by purchase volume."""
        pedidos_total = cls.calcular_valor_total_pedido(pedidos)
        agregado = cls.agregar_por_cliente(pedidos_total)
        ranking = cls.rankear_top_n(agregado, n=10)
        resultado = cls.enriquecer_com_clientes(ranking, clientes)
        return resultado.orderBy("RANKING")
