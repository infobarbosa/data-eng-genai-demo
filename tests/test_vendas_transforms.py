import pytest
from pyspark.sql import SparkSession

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_vendas_transforms")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def pedidos_df(spark):
    data = [
        ("ped-1", "NOTEBOOK", 1500.0, 2, "2026-01-01T10:00:00", "SP", 1),
        ("ped-2", "CELULAR", 1000.0, 3, "2026-01-02T11:00:00", "RJ", 2),
        ("ped-3", "GELADEIRA", 2000.0, 1, "2026-01-03T12:00:00", "MG", 1),
        ("ped-4", "TV", 2500.0, 1, "2026-01-04T13:00:00", "BA", 3),
        ("ped-5", "NOTEBOOK", 1500.0, 1, "2026-01-05T14:00:00", "SP", 4),
        ("ped-6", "CELULAR", 1000.0, 2, "2026-01-06T15:00:00", "RJ", 5),
        ("ped-7", "LIQUIDIFICADOR", 300.0, 3, "2026-01-07T16:00:00", "DF", 6),
        ("ped-8", "NOTEBOOK", 1500.0, 2, "2026-01-08T17:00:00", "SP", 7),
        ("ped-9", "TV", 2500.0, 2, "2026-01-09T18:00:00", "MG", 8),
        ("ped-10", "GELADEIRA", 2000.0, 3, "2026-01-10T19:00:00", "BA", 9),
        ("ped-11", "CELULAR", 1000.0, 5, "2026-01-11T20:00:00", "SP", 10),
        ("ped-12", "NOTEBOOK", 1500.0, 4, "2026-01-12T21:00:00", "RJ", 11),
    ]
    columns = [
        "ID_PEDIDO",
        "PRODUTO",
        "VALOR_UNITARIO",
        "QUANTIDADE",
        "DATA_CRIACAO",
        "UF",
        "ID_CLIENTE",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture()
def clientes_df(spark):
    data = [
        (1, "Alice Silva", "alice@email.com"),
        (2, "Bob Santos", "bob@email.com"),
        (3, "Carlos Lima", "carlos@email.com"),
        (4, "Diana Costa", "diana@email.com"),
        (5, "Eva Souza", "eva@email.com"),
        (6, "Felipe Rocha", "felipe@email.com"),
        (7, "Gabriela Dias", "gabriela@email.com"),
        (8, "Henrique Alves", "henrique@email.com"),
        (9, "Isabela Ferreira", "isabela@email.com"),
        (10, "João Oliveira", "joao@email.com"),
        (11, "Karen Ribeiro", "karen@email.com"),
    ]
    columns = ["id", "nome", "email"]
    return spark.createDataFrame(data, columns)


class TestCalcularValorTotalPedido:
    def test_adds_valor_total_column(self, pedidos_df):
        result = VendasTransforms.calcular_valor_total_pedido(pedidos_df)
        assert "VALOR_TOTAL" in result.columns

    def test_correct_calculation(self, pedidos_df):
        result = VendasTransforms.calcular_valor_total_pedido(pedidos_df)
        row = result.filter(result["ID_PEDIDO"] == "ped-1").collect()[0]
        assert row["VALOR_TOTAL"] == 3000.0  # 1500 * 2


class TestAgregarPorCliente:
    def test_aggregation(self, pedidos_df):
        with_total = VendasTransforms.calcular_valor_total_pedido(pedidos_df)
        result = VendasTransforms.agregar_por_cliente(with_total)
        assert "TOTAL_COMPRAS" in result.columns
        assert "QTD_PEDIDOS" in result.columns

    def test_cliente_with_multiple_orders(self, pedidos_df):
        with_total = VendasTransforms.calcular_valor_total_pedido(pedidos_df)
        result = VendasTransforms.agregar_por_cliente(with_total)
        # Client 1 has ped-1 (3000) + ped-3 (2000) = 5000
        row = result.filter(result["ID_CLIENTE"] == 1).collect()[0]
        assert row["TOTAL_COMPRAS"] == 5000.0
        assert row["QTD_PEDIDOS"] == 2


class TestRankearTopN:
    def test_returns_top_n(self, pedidos_df):
        with_total = VendasTransforms.calcular_valor_total_pedido(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(with_total)
        result = VendasTransforms.rankear_top_n(agregado, n=3)
        assert result.count() == 3

    def test_ranking_order(self, pedidos_df):
        with_total = VendasTransforms.calcular_valor_total_pedido(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(with_total)
        result = VendasTransforms.rankear_top_n(agregado, n=3)
        rows = result.collect()
        assert rows[0]["RANKING"] == 1
        assert rows[1]["RANKING"] == 2
        assert rows[2]["RANKING"] == 3
        # First should have highest total
        assert rows[0]["TOTAL_COMPRAS"] >= rows[1]["TOTAL_COMPRAS"]


class TestEnriquecerComClientes:
    def test_join_adds_nome_and_email(self, pedidos_df, clientes_df):
        with_total = VendasTransforms.calcular_valor_total_pedido(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(with_total)
        ranking = VendasTransforms.rankear_top_n(agregado, n=3)
        result = VendasTransforms.enriquecer_com_clientes(ranking, clientes_df)
        assert "nome" in result.columns
        assert "email" in result.columns


class TestTop10Clientes:
    def test_full_pipeline(self, pedidos_df, clientes_df):
        result = VendasTransforms.top_10_clientes(pedidos_df, clientes_df)
        # We have 11 distinct clients; top 10 should be returned
        assert result.count() == 10
        assert "RANKING" in result.columns
        assert "nome" in result.columns
        rows = result.collect()
        assert rows[0]["RANKING"] == 1
