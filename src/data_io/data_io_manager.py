"""Modulo responsavel pela leitura e escrita de dados via catalogo (sem frameworks)."""

import csv
import glob
import json
import logging
import os
import shutil
from typing import Any, Dict, List

from src.core.exceptions import DataIOError


class DataIOManager:
    """Gerencia operacoes de leitura e escrita de dados utilizando o catalogo de configuracao."""

    def __init__(self, catalogo: Dict[str, Dict[str, Any]], base_path: str) -> None:
        self._catalogo = catalogo
        self._base_path = base_path
        self._logger = logging.getLogger("top10_pipeline.DataIOManager")

    def _resolver_caminho(self, caminho_relativo: str) -> str:
        """Resolve o caminho fisico a partir do caminho relativo no catalogo."""
        return os.path.join(self._base_path, caminho_relativo)

    def ler(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Le um dataset pelo seu ID logico no catalogo.

        Retorna uma lista de dicionarios (cada dicionario e uma linha/registro).
        """
        if dataset_id not in self._catalogo:
            raise DataIOError(f"Dataset '{dataset_id}' nao encontrado no catalogo.")

        dataset_config = self._catalogo[dataset_id]
        caminho = self._resolver_caminho(dataset_config["caminho"])
        formato = dataset_config["formato"]
        opcoes = dataset_config.get("opcoes", {})

        self._logger.info("Lendo dataset '%s' de '%s' (formato: %s)", dataset_id, caminho, formato)

        try:
            if formato == "json":
                dados = self._ler_json(caminho)
            elif formato == "csv":
                sep = opcoes.get("sep", ",")
                dados = self._ler_csv(caminho, sep=sep)
            else:
                raise DataIOError(f"Formato '{formato}' nao suportado.")

            self._logger.info("Dataset '%s' carregado com %d registros.", dataset_id, len(dados))
            return dados
        except DataIOError:
            raise
        except Exception as e:
            raise DataIOError(f"Erro ao ler dataset '{dataset_id}' de '{caminho}': {e}")

    def _ler_json(self, caminho: str) -> List[Dict[str, Any]]:
        """Le um arquivo JSON lines (um objeto JSON por linha)."""
        dados: List[Dict[str, Any]] = []
        if os.path.isfile(caminho):
            with open(caminho, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        dados.append(json.loads(line))
        elif os.path.isdir(caminho):
            for arquivo in sorted(glob.glob(os.path.join(caminho, "**/*.json"), recursive=True)):
                with open(arquivo, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            dados.append(json.loads(line))
        else:
            raise DataIOError(f"Caminho nao encontrado: {caminho}")
        return dados

    def _ler_csv(self, caminho: str, sep: str = ",") -> List[Dict[str, Any]]:
        """Le arquivos CSV de um diretorio ou arquivo unico."""
        dados: List[Dict[str, Any]] = []
        arquivos: List[str] = []

        if os.path.isfile(caminho):
            arquivos.append(caminho)
        elif os.path.isdir(caminho):
            arquivos = sorted(glob.glob(os.path.join(caminho, "**/*.csv"), recursive=True))
        else:
            raise DataIOError(f"Caminho nao encontrado: {caminho}")

        for arquivo in arquivos:
            with open(arquivo, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=sep)
                for row in reader:
                    dados.append(dict(row))
        return dados

    def escrever(self, dados: List[Dict[str, Any]], dataset_id: str) -> None:
        """Escreve uma lista de dicionarios no destino definido pelo ID logico no catalogo."""
        if dataset_id not in self._catalogo:
            raise DataIOError(f"Dataset '{dataset_id}' nao encontrado no catalogo.")

        dataset_config = self._catalogo[dataset_id]
        caminho = self._resolver_caminho(dataset_config["caminho"])
        formato = dataset_config.get("formato", "csv")
        modo = dataset_config.get("modo", "overwrite")

        self._logger.info("Escrevendo dataset '%s' em '%s' (formato: %s, modo: %s)", dataset_id, caminho, formato, modo)

        try:
            if modo == "overwrite" and os.path.exists(caminho):
                if os.path.isdir(caminho):
                    shutil.rmtree(caminho)
                else:
                    os.remove(caminho)

            os.makedirs(caminho, exist_ok=True)
            arquivo_saida = os.path.join(caminho, "resultado.csv")

            if not dados:
                with open(arquivo_saida, "w", encoding="utf-8") as f:
                    f.write("")
                self._logger.info("Dataset '%s' salvo (vazio).", dataset_id)
                return

            fieldnames = list(dados[0].keys())
            with open(arquivo_saida, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
                writer.writeheader()
                writer.writerows(dados)

            self._logger.info("Dataset '%s' salvo com sucesso em '%s'.", dataset_id, arquivo_saida)
        except Exception as e:
            raise DataIOError(f"Erro ao escrever dataset '{dataset_id}' em '{caminho}': {e}")
