"""Testes unitarios para as transformacoes de vendas (sem frameworks)."""

import pytest

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture(scope="module")
def pedidos_data():
    """Cria uma lista de dicionarios sinteticos de pedidos."""
    return [
        {
            "ID_PEDIDO": "ped-001",
            "PRODUTO": "NOTEBOOK",
            "VALOR_UNITARIO": 1500.0,
            "QUANTIDADE": 2,
            "DATA_CRIACAO": "2026-01-01T10:00:00",
            "UF": "SP",
            "ID_CLIENTE": 1,
        },
        {
            "ID_PEDIDO": "ped-002",
            "PRODUTO": "CELULAR",
            "VALOR_UNITARIO": 1000.0,
            "QUANTIDADE": 3,
            "DATA_CRIACAO": "2026-01-02T11:00:00",
            "UF": "RJ",
            "ID_CLIENTE": 2,
        },
        {
            "ID_PEDIDO": "ped-003",
            "PRODUTO": "GELADEIRA",
            "VALOR_UNITARIO": 2000.0,
            "QUANTIDADE": 1,
            "DATA_CRIACAO": "2026-01-03T12:00:00",
            "UF": "MG",
            "ID_CLIENTE": 1,
        },
        {
            "ID_PEDIDO": "ped-004",
            "PRODUTO": "TV",
            "VALOR_UNITARIO": 2500.0,
            "QUANTIDADE": 1,
            "DATA_CRIACAO": "2026-01-04T13:00:00",
            "UF": "SP",
            "ID_CLIENTE": 3,
        },
        {
            "ID_PEDIDO": "ped-005",
            "PRODUTO": "LIQUIDIFICADOR",
            "VALOR_UNITARIO": 300.0,
            "QUANTIDADE": 5,
            "DATA_CRIACAO": "2026-01-05T14:00:00",
            "UF": "BA",
            "ID_CLIENTE": 2,
        },
        {
            "ID_PEDIDO": "ped-006",
            "PRODUTO": "NOTEBOOK",
            "VALOR_UNITARIO": 1500.0,
            "QUANTIDADE": 1,
            "DATA_CRIACAO": "2026-01-06T15:00:00",
            "UF": "RS",
            "ID_CLIENTE": 4,
        },
        {
            "ID_PEDIDO": "ped-007",
            "PRODUTO": "CELULAR",
            "VALOR_UNITARIO": 1000.0,
            "QUANTIDADE": 2,
            "DATA_CRIACAO": "2026-01-07T16:00:00",
            "UF": "PR",
            "ID_CLIENTE": 5,
        },
        {
            "ID_PEDIDO": "ped-008",
            "PRODUTO": "GELADEIRA",
            "VALOR_UNITARIO": 2000.0,
            "QUANTIDADE": 3,
            "DATA_CRIACAO": "2026-01-08T17:00:00",
            "UF": "SC",
            "ID_CLIENTE": 6,
        },
        {
            "ID_PEDIDO": "ped-009",
            "PRODUTO": "TV",
            "VALOR_UNITARIO": 2500.0,
            "QUANTIDADE": 2,
            "DATA_CRIACAO": "2026-01-09T18:00:00",
            "UF": "AM",
            "ID_CLIENTE": 7,
        },
        {
            "ID_PEDIDO": "ped-010",
            "PRODUTO": "NOTEBOOK",
            "VALOR_UNITARIO": 1500.0,
            "QUANTIDADE": 4,
            "DATA_CRIACAO": "2026-01-10T19:00:00",
            "UF": "CE",
            "ID_CLIENTE": 8,
        },
        {
            "ID_PEDIDO": "ped-011",
            "PRODUTO": "CELULAR",
            "VALOR_UNITARIO": 1000.0,
            "QUANTIDADE": 1,
            "DATA_CRIACAO": "2026-01-11T20:00:00",
            "UF": "PE",
            "ID_CLIENTE": 9,
        },
        {
            "ID_PEDIDO": "ped-012",
            "PRODUTO": "GELADEIRA",
            "VALOR_UNITARIO": 2000.0,
            "QUANTIDADE": 2,
            "DATA_CRIACAO": "2026-01-12T21:00:00",
            "UF": "GO",
            "ID_CLIENTE": 10,
        },
        {
            "ID_PEDIDO": "ped-013",
            "PRODUTO": "TV",
            "VALOR_UNITARIO": 2500.0,
            "QUANTIDADE": 3,
            "DATA_CRIACAO": "2026-01-13T22:00:00",
            "UF": "MT",
            "ID_CLIENTE": 11,
        },
        {
            "ID_PEDIDO": "ped-014",
            "PRODUTO": "LIQUIDIFICADOR",
            "VALOR_UNITARIO": 300.0,
            "QUANTIDADE": 10,
            "DATA_CRIACAO": "2026-01-14T23:00:00",
            "UF": "MS",
            "ID_CLIENTE": 12,
        },
    ]


@pytest.fixture(scope="module")
def clientes_data():
    """Cria uma lista de dicionarios sinteticos de clientes."""
    return [
        {"id": 1, "nome": "Ana Silva", "data_nasc": "1990-01-01", "cpf": "111.222.333-44", "email": "ana@email.com"},
        {
            "id": 2,
            "nome": "Bruno Costa",
            "data_nasc": "1985-05-10",
            "cpf": "222.333.444-55",
            "email": "bruno@email.com",
        },
        {"id": 3, "nome": "Carla Dias", "data_nasc": "1992-03-15", "cpf": "333.444.555-66", "email": "carla@email.com"},
        {
            "id": 4,
            "nome": "Daniel Souza",
            "data_nasc": "1988-07-20",
            "cpf": "444.555.666-77",
            "email": "daniel@email.com",
        },
        {
            "id": 5,
            "nome": "Elena Martins",
            "data_nasc": "1995-11-30",
            "cpf": "555.666.777-88",
            "email": "elena@email.com",
        },
    ]


class TestCalcularValorTotal:
    """Testes para a funcao calcular_valor_total."""

    def test_valor_total_calculado_corretamente(self, pedidos_data):
        resultado = VendasTransforms.calcular_valor_total(pedidos_data)
        assert all("VALOR_TOTAL" in r for r in resultado)

        # Primeiro pedido: 1500 * 2 = 3000
        primeiro = [r for r in resultado if r["ID_PEDIDO"] == "ped-001"][0]
        assert primeiro["VALOR_TOTAL"] == 3000.0

    def test_valor_total_multiplicacao(self, pedidos_data):
        resultado = VendasTransforms.calcular_valor_total(pedidos_data)

        # ped-005: 300 * 5 = 1500
        registro = [r for r in resultado if r["ID_PEDIDO"] == "ped-005"][0]
        assert registro["VALOR_TOTAL"] == 1500.0


class TestAgregarPorCliente:
    """Testes para a funcao agregar_por_cliente."""

    def test_agregacao_soma_correta(self, pedidos_data):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_data)
        resultado = VendasTransforms.agregar_por_cliente(pedidos_com_total)

        # Cliente 1: (1500*2) + (2000*1) = 5000
        cliente_1 = [r for r in resultado if r["ID_CLIENTE"] == 1][0]
        assert cliente_1["TOTAL_COMPRAS"] == 5000.0
        assert cliente_1["QTD_PEDIDOS"] == 2

    def test_agregacao_cliente_unico(self, pedidos_data):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_data)
        resultado = VendasTransforms.agregar_por_cliente(pedidos_com_total)

        # Cliente 3: 2500*1 = 2500
        cliente_3 = [r for r in resultado if r["ID_CLIENTE"] == 3][0]
        assert cliente_3["TOTAL_COMPRAS"] == 2500.0
        assert cliente_3["QTD_PEDIDOS"] == 1


class TestRankearTopN:
    """Testes para a funcao rankear_top_n."""

    def test_top_3_retorna_3_registros(self, pedidos_data):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_data)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        resultado = VendasTransforms.rankear_top_n(agregado, n=3)

        assert len(resultado) == 3

    def test_ranking_ordenado_decrescente(self, pedidos_data):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_data)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        resultado = VendasTransforms.rankear_top_n(agregado, n=5)

        valores = [r["TOTAL_COMPRAS"] for r in resultado]
        assert valores == sorted(valores, reverse=True)

    def test_ranking_coluna_presente(self, pedidos_data):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_data)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        resultado = VendasTransforms.rankear_top_n(agregado, n=3)

        assert all("RANKING" in r for r in resultado)
        rankings = [r["RANKING"] for r in resultado]
        assert rankings == [1, 2, 3]


class TestEnriquecerComClientes:
    """Testes para a funcao enriquecer_com_clientes."""

    def test_join_com_clientes(self, pedidos_data, clientes_data):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_data)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        ranking = VendasTransforms.rankear_top_n(agregado, n=5)
        resultado = VendasTransforms.enriquecer_com_clientes(ranking, clientes_data)

        assert all("NOME_CLIENTE" in r for r in resultado)
        assert all("EMAIL" in r for r in resultado)

    def test_dados_cliente_corretos(self, pedidos_data, clientes_data):
        pedidos_com_total = VendasTransforms.calcular_valor_total(pedidos_data)
        agregado = VendasTransforms.agregar_por_cliente(pedidos_com_total)
        ranking = VendasTransforms.rankear_top_n(agregado, n=12)
        resultado = VendasTransforms.enriquecer_com_clientes(ranking, clientes_data)

        # Cliente 1 deve ter nome "Ana Silva"
        cliente_1 = [r for r in resultado if r["ID_CLIENTE"] == 1][0]
        assert cliente_1["NOME_CLIENTE"] == "Ana Silva"
        assert cliente_1["EMAIL"] == "ana@email.com"
