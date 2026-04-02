"""Testes unitarios para as transformacoes de vendas."""

import pytest
from pyspark.sql import SparkSession

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture(scope="module")
def spark():
    """Cria uma SparkSession para os testes."""
    session = SparkSession.builder.appName("TestVendasTransforms").master("local[*]").getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def pedidos_df(spark):
    """Cria um DataFrame sintetico de pedidos."""
    dados = [
        ("ped-001", "NOTEBOOK", 1500.0, 2, "2026-01-01T10:00:00", "SP", 1),
        ("ped-002", "CELULAR", 1000.0, 3, "2026-01-02T11:00:00", "RJ", 2),
        ("ped-003", "GELADEIRA", 2000.0, 1, "2026-01-03T12:00:00", "MG", 1),
        ("ped-004", "TV", 2500.0, 1, "2026-01-04T13:00:00", "SP", 3),
        ("ped-005", "LIQUIDIFICADOR", 300.0, 5, "2026-01-05T14:00:00", "BA", 2),
        ("ped-006", "NOTEBOOK", 1500.0, 1, "2026-01-06T15:00:00", "RS", 4),
        ("ped-007", "CELULAR", 1000.0, 2, "2026-01-07T16:00:00", "PR", 5),
        ("ped-008", "GELADEIRA", 2000.0, 3, "2026-01-08T17:00:00", "SC", 6),
        ("ped-009", "TV", 2500.0, 2, "2026-01-09T18:00:00", "AM", 7),
        ("ped-010", "NOTEBOOK", 1500.0, 4, "2026-01-10T19:00:00", "CE", 8),
        ("ped-011", "CELULAR", 1000.0, 1, "2026-01-11T20:00:00", "PE", 9),
        ("ped-012", "GELADEIRA", 2000.0, 2, "2026-01-12T21:00:00", "GO", 10),
        ("ped-013", "TV", 2500.0, 3, "2026-01-13T22:00:00", "MT", 11),
        ("ped-014", "LIQUIDIFICADOR", 300.0, 10, "2026-01-14T23:00:00", "MS", 12),
    ]
    colunas = ["ID_PEDIDO", "PRODUTO", "VALOR_UNITARIO", "QUANTIDADE", "DATA_CRIACAO", "UF", "ID_CLIENTE"]
    return spark.createDataFrame(dados, colunas)


@pytest.fixture(scope="module")
def clientes_df(spark):
    """Cria um DataFrame sintetico de clientes."""
    dados = [
        (1, "Ana Silva", "1990-01-01", "111.222.333-44", "ana@email.com"),
        (2, "Bruno Costa", "1985-05-10", "222.333.444-55", "bruno@email.com"),
        (3, "Carla Dias", "1992-03-15", "333.444.555-66", "carla@email.com"),
        (4, "Daniel Souza", "1988-07-20", "444.555.666-77", "daniel@email.com"),
        (5, "Elena Martins", "1995-11-30", "555.666.777-88", "elena@email.com"),
    ]
    colunas = ["id", "nome", "data_nasc", "cpf", "email"]
    return spark.createDataFrame(dados, colunas)


class TestCalcularValorTotal:
    """Testes para a funcao calcular_valor_total."""

    def test_valor_total_calculado_corretamente(self, pedidos_df):
        resultado = VendasTransforms.calcular_valor_total(pedidos_df)
        assert "VALOR_TOTAL" in resultado.columns

        # Primeiro pedido: 1500 * 2 = 3000
        primeiro = resultado.filter(resultado["ID_PEDIDO"] == "ped-001").collect()[0]
        assert primeiro["VALOR_TOTAL"] == 3000.0

    def test_valor_total_multiplicacao(self, pedidos_df):
        resultado = VendasTransforms.calcular_valor_total(pedidos_df)

        # ped-005: 300 * 5 = 1500
        registro = resultado.filter(resultado["ID_PEDIDO"] == "ped-005").collect()[0]
        assert registro["VALOR_TOTAL"] == 1500.0


class TestAgregarPorCliente:
    """Testes para a funcao agregar_por_cliente."""

    def test_agregacao_soma_correta(self, pedidos_df):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)
        resultado = VendasTransforms.agregar_por_cliente(pedidos_com_total)

        # Cliente 1: (1500*2) + (2000*1) = 5000
        cliente_1 = resultado.filter(resultado["ID_CLIENTE"] == 1).collect()[0]
        assert cliente_1["TOTAL_COMPRAS"] == 5000.0
        assert cliente_1["QTD_PEDIDOS"] == 2

    def test_agregacao_cliente_unico(self, pedidos_df):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)
        resultado = VendasTransforms.agregar_por_cliente(pedidos_com_total)

        # Cliente 3: 2500*1 = 2500
        cliente_3 = resultado.filter(resultado["ID_CLIENTE"] == 3).collect()[0]
        assert cliente_3["TOTAL_COMPRAS"] == 2500.0
        assert cliente_3["QTD_PEDIDOS"] == 1


class TestRankearTopN:
    """Testes para a funcao rankear_top_n."""

    def test_top_3_retorna_3_registros(self, pedidos_df):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        resultado = VendasTransforms.rankear_top_n(agregado, n=3)

        assert resultado.count() == 3

    def test_ranking_ordenado_decrescente(self, pedidos_df):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        resultado = VendasTransforms.rankear_top_n(agregado, n=5)

        valores = [row["TOTAL_COMPRAS"] for row in resultado.collect()]
        assert valores == sorted(valores, reverse=True)

    def test_ranking_coluna_presente(self, pedidos_df):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        resultado = VendasTransforms.rankear_top_n(agregado, n=3)

        assert "RANKING" in resultado.columns
        rankings = [row["RANKING"] for row in resultado.collect()]
        assert rankings == [1, 2, 3]


class TestEnriquecerComClientes:
    """Testes para a funcao enriquecer_com_clientes."""

    def test_join_com_clientes(self, pedidos_df, clientes_df):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        ranking = VendasTransforms.rankear_top_n(agregado, n=5)
        resultado = VendasTransforms.enriquecer_com_clientes(ranking, clientes_df)

        assert "NOME_CLIENTE" in resultado.columns
        assert "EMAIL" in resultado.columns

    def test_dados_cliente_corretos(self, pedidos_df, clientes_df):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_df)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        ranking = VendasTransforms.rankear_top_n(agregado, n=12)
        resultado = VendasTransforms.enriquecer_com_clientes(ranking, clientes_df)

        # Cliente 1 deve ter nome "Ana Silva"
        cliente_1 = resultado.filter(resultado["ID_CLIENTE"] == 1).collect()[0]
        assert cliente_1["NOME_CLIENTE"] == "Ana Silva"
        assert cliente_1["EMAIL"] == "ana@email.com"
