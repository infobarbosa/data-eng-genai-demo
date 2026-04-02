"""Testes unitarios para VendasTransforms."""

import pandas as pd
import pytest

from src.core.exceptions import TransformError
from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture
def transforms():
    """Instancia de VendasTransforms para testes."""
    return VendasTransforms()


@pytest.fixture
def df_pedidos():
    """DataFrame sintetico de pedidos."""
    return pd.DataFrame(
        {
            "ID_PEDIDO": ["p1", "p2", "p3", "p4", "p5", "p6"],
            "PRODUTO": [
                "NOTEBOOK",
                "CELULAR",
                "GELADEIRA",
                "NOTEBOOK",
                "CELULAR",
                "TV",
            ],
            "VALOR_UNITARIO": [1500.0, 1000.0, 2000.0, 1500.0, 1000.0, 3000.0],
            "QUANTIDADE": [2, 3, 1, 1, 2, 1],
            "DATA_CRIACAO": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
                "2026-01-06",
            ],
            "UF": ["SP", "RJ", "MG", "SP", "RJ", "BA"],
            "ID_CLIENTE": [1, 2, 3, 1, 2, 4],
        }
    )


@pytest.fixture
def df_clientes():
    """DataFrame sintetico de clientes."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "nome": [
                "Alice Silva",
                "Bruno Santos",
                "Carla Oliveira",
                "Daniel Costa",
                "Eva Lima",
            ],
            "data_nasc": [
                "1990-01-01",
                "1985-05-15",
                "1992-08-20",
                "1988-03-10",
                "1995-12-25",
            ],
            "cpf": [
                "111.111.111-11",
                "222.222.222-22",
                "333.333.333-33",
                "444.444.444-44",
                "555.555.555-55",
            ],
            "email": [
                "alice@email.com",
                "bruno@email.com",
                "carla@email.com",
                "daniel@email.com",
                "eva@email.com",
            ],
        }
    )


class TestCalcularValorTotal:
    """Testes para o metodo calcular_valor_total."""

    def test_calcula_valor_total_corretamente(self, transforms, df_pedidos):
        result = transforms.calcular_valor_total(df_pedidos)
        assert "VALOR_TOTAL" in result.columns
        expected = [3000.0, 3000.0, 2000.0, 1500.0, 2000.0, 3000.0]
        assert result["VALOR_TOTAL"].tolist() == expected

    def test_nao_altera_dataframe_original(self, transforms, df_pedidos):
        original_cols = list(df_pedidos.columns)
        transforms.calcular_valor_total(df_pedidos)
        assert list(df_pedidos.columns) == original_cols

    def test_erro_sem_colunas_obrigatorias(self, transforms):
        df = pd.DataFrame({"PRODUTO": ["A"], "PRECO": [10.0]})
        with pytest.raises(TransformError, match="Colunas obrigatorias ausentes"):
            transforms.calcular_valor_total(df)


class TestAgregarPorCliente:
    """Testes para o metodo agregar_por_cliente."""

    def test_agrega_corretamente(self, transforms, df_pedidos):
        df_com_total = transforms.calcular_valor_total(df_pedidos)
        result = transforms.agregar_por_cliente(df_com_total)

        assert len(result) == 4
        assert "ID_CLIENTE" in result.columns
        assert "VALOR_TOTAL_COMPRAS" in result.columns

        cliente_1 = result[result["ID_CLIENTE"] == 1]["VALOR_TOTAL_COMPRAS"].iloc[0]
        assert cliente_1 == 4500.0  # 3000 + 1500

        cliente_2 = result[result["ID_CLIENTE"] == 2]["VALOR_TOTAL_COMPRAS"].iloc[0]
        assert cliente_2 == 5000.0  # 3000 + 2000

    def test_erro_sem_colunas_obrigatorias(self, transforms):
        df = pd.DataFrame({"PRODUTO": ["A"], "PRECO": [10.0]})
        with pytest.raises(TransformError, match="Colunas obrigatorias ausentes"):
            transforms.agregar_por_cliente(df)


class TestTopNClientes:
    """Testes para o metodo top_n_clientes."""

    def test_retorna_top_n_correto(self, transforms, df_pedidos, df_clientes):
        result = transforms.top_n_clientes(df_pedidos, df_clientes, n=3)

        assert len(result) == 3
        assert result.index.name == "RANKING"

        # Primeiro lugar: Bruno Santos (ID 2) com 5000
        assert result.iloc[0]["nome"] == "Bruno Santos"
        assert result.iloc[0]["VALOR_TOTAL_COMPRAS"] == 5000.0

        # Segundo lugar: Alice Silva (ID 1) com 4500
        assert result.iloc[1]["nome"] == "Alice Silva"
        assert result.iloc[1]["VALOR_TOTAL_COMPRAS"] == 4500.0

    def test_retorna_top_2(self, transforms, df_pedidos, df_clientes):
        result = transforms.top_n_clientes(df_pedidos, df_clientes, n=2)
        assert len(result) == 2

    def test_top_n_maior_que_clientes(self, transforms, df_pedidos, df_clientes):
        result = transforms.top_n_clientes(df_pedidos, df_clientes, n=100)
        # So existem 4 clientes com pedidos que estao no df_clientes
        assert len(result) == 4

    def test_colunas_resultado(self, transforms, df_pedidos, df_clientes):
        result = transforms.top_n_clientes(df_pedidos, df_clientes, n=3)
        expected_cols = {"ID_CLIENTE", "VALOR_TOTAL_COMPRAS", "nome"}
        assert set(result.columns) == expected_cols

    def test_ordenacao_decrescente(self, transforms, df_pedidos, df_clientes):
        result = transforms.top_n_clientes(df_pedidos, df_clientes, n=4)
        valores = result["VALOR_TOTAL_COMPRAS"].tolist()
        assert valores == sorted(valores, reverse=True)
