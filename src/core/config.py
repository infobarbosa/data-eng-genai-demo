"""Modulo responsavel por carregar e validar a configuracao do pipeline."""

import os
import re
from typing import Any, Dict

from src.core.exceptions import ConfigError


class ConfigLoader:
    """Carrega e fornece acesso a configuracao do pipeline a partir de um arquivo YAML."""

    def __init__(self, config_path: str) -> None:
        self._config_path = config_path
        self._config: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Carrega o arquivo YAML de configuracao usando parser simplificado."""
        if not os.path.exists(self._config_path):
            raise ConfigError(f"Arquivo de configuracao nao encontrado: {self._config_path}")

        try:
            with open(self._config_path, "r", encoding="utf-8") as f:
                content = f.read()
            self._config = self._parse_yaml(content)
        except ConfigError:
            raise
        except Exception as e:
            raise ConfigError(f"Erro ao parsear o arquivo YAML: {e}")

        if not self._config:
            raise ConfigError("Arquivo de configuracao esta vazio.")

    @staticmethod
    def _parse_yaml(content: str) -> Dict[str, Any]:
        """Parser YAML simplificado para estruturas com ate 3 niveis de indentacao.

        Suporta:
        - Chaves simples com valores string, int, float
        - Valores entre aspas
        - Dicionarios aninhados (ate 3 niveis)
        """
        result: Dict[str, Any] = {}
        lines = content.split("\n")

        # Pilha para rastrear o contexto atual de aninhamento
        # Cada entrada: (indent_level, dict_reference)
        stack: list = [(0, result)]

        for line in lines:
            # Ignorar linhas vazias e comentarios
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            # Calcular nivel de indentacao
            indent = len(line) - len(line.lstrip())

            # Extrair chave e valor
            match = re.match(r"^(\s*)(\w[\w_]*):\s*(.*?)\s*$", line)
            if not match:
                continue

            key = match.group(2)
            value_str = match.group(3)

            # Desempilhar ate encontrar o nivel pai correto
            while len(stack) > 1 and stack[-1][0] >= indent:
                stack.pop()

            parent_dict = stack[-1][1]

            if value_str:
                # Tem valor na mesma linha
                parent_dict[key] = ConfigLoader._parse_value(value_str)
            else:
                # E um dicionario aninhado
                new_dict: Dict[str, Any] = {}
                parent_dict[key] = new_dict
                stack.append((indent, new_dict))

        return result

    @staticmethod
    def _parse_value(value_str: str) -> Any:
        """Converte uma string de valor YAML para o tipo Python apropriado."""
        # Remover aspas
        if (value_str.startswith('"') and value_str.endswith('"')) or (
            value_str.startswith("'") and value_str.endswith("'")
        ):
            return value_str[1:-1]

        # Tentar converter para int
        try:
            return int(value_str)
        except ValueError:
            pass

        # Tentar converter para float
        try:
            return float(value_str)
        except ValueError:
            pass

        # Booleanos
        if value_str.lower() == "true":
            return True
        if value_str.lower() == "false":
            return False

        # Retornar como string
        return value_str

    @property
    def spark_config(self) -> Dict[str, str]:
        """Retorna a configuracao do Spark (mantida por compatibilidade)."""
        spark = self._config.get("spark")
        if not spark:
            raise ConfigError("Secao 'spark' nao encontrada na configuracao.")
        return spark

    @property
    def catalogo(self) -> Dict[str, Dict[str, Any]]:
        """Retorna o catalogo de dados."""
        catalogo = self._config.get("catalogo")
        if not catalogo:
            raise ConfigError("Secao 'catalogo' nao encontrada na configuracao.")
        return catalogo

    @property
    def pipeline_config(self) -> Dict[str, Any]:
        """Retorna a configuracao do pipeline."""
        pipeline = self._config.get("pipeline")
        if not pipeline:
            raise ConfigError("Secao 'pipeline' nao encontrada na configuracao.")
        return pipeline

    def get_dataset_config(self, dataset_id: str) -> Dict[str, Any]:
        """Retorna a configuracao de um dataset especifico pelo seu ID logico."""
        catalogo = self.catalogo
        if dataset_id not in catalogo:
            raise ConfigError(f"Dataset '{dataset_id}' nao encontrado no catalogo.")
        return catalogo[dataset_id]
