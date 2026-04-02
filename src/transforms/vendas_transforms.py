"""Modulo com logica pura de transformacao para o ranking de clientes (sem frameworks)."""

from collections import defaultdict
from typing import Any, Dict, List


class VendasTransforms:
    """Transformacoes puras sobre listas de dicionarios de vendas/pedidos."""

    @staticmethod
    def calcular_valor_total(pedidos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Adiciona a chave VALOR_TOTAL = VALOR_UNITARIO * QUANTIDADE a cada registro."""
        resultado: List[Dict[str, Any]] = []
        for pedido in pedidos:
            novo = dict(pedido)
            valor_unitario = float(novo["VALOR_UNITARIO"])
            quantidade = int(novo["QUANTIDADE"])
            novo["VALOR_TOTAL"] = valor_unitario * quantidade
            resultado.append(novo)
        return resultado

    @staticmethod
    def agregar_por_cliente(pedidos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Agrupa por ID_CLIENTE e soma o VALOR_TOTAL e conta pedidos."""
        totais: Dict[Any, float] = defaultdict(float)
        contagens: Dict[Any, int] = defaultdict(int)

        for pedido in pedidos:
            cliente_id = pedido["ID_CLIENTE"]
            totais[cliente_id] += pedido["VALOR_TOTAL"]
            contagens[cliente_id] += 1

        resultado: List[Dict[str, Any]] = []
        for cliente_id in totais:
            resultado.append(
                {
                    "ID_CLIENTE": cliente_id,
                    "TOTAL_COMPRAS": totais[cliente_id],
                    "QTD_PEDIDOS": contagens[cliente_id],
                }
            )
        return resultado

    @staticmethod
    def rankear_top_n(agregado: List[Dict[str, Any]], n: int = 10) -> List[Dict[str, Any]]:
        """Ordena por TOTAL_COMPRAS decrescente e retorna os top N clientes com ranking."""
        ordenado = sorted(agregado, key=lambda x: x["TOTAL_COMPRAS"], reverse=True)
        top_n = ordenado[:n]

        resultado: List[Dict[str, Any]] = []
        for i, registro in enumerate(top_n, start=1):
            novo = dict(registro)
            novo["RANKING"] = i
            resultado.append(novo)
        return resultado

    @staticmethod
    def enriquecer_com_clientes(ranking: List[Dict[str, Any]], clientes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Faz join do ranking com dados de clientes para enriquecer o resultado."""
        # Criar lookup de clientes por id
        clientes_lookup: Dict[Any, Dict[str, Any]] = {}
        for cliente in clientes:
            cliente_id = cliente.get("id")
            clientes_lookup[cliente_id] = cliente

        resultado: List[Dict[str, Any]] = []
        for registro in ranking:
            cliente_id = registro["ID_CLIENTE"]
            # Tentar buscar como int e como string
            cliente_info = clientes_lookup.get(cliente_id) or clientes_lookup.get(str(cliente_id))
            if cliente_id not in clientes_lookup and str(cliente_id) not in clientes_lookup:
                # Tentar converter para int
                try:
                    cliente_info = clientes_lookup.get(int(cliente_id))
                except (ValueError, TypeError):
                    cliente_info = None

            novo: Dict[str, Any] = {
                "RANKING": registro["RANKING"],
                "ID_CLIENTE": registro["ID_CLIENTE"],
                "NOME_CLIENTE": cliente_info.get("nome", "") if cliente_info else "",
                "EMAIL": cliente_info.get("email", "") if cliente_info else "",
                "TOTAL_COMPRAS": registro["TOTAL_COMPRAS"],
                "QTD_PEDIDOS": registro["QTD_PEDIDOS"],
            }
            resultado.append(novo)
        return resultado
