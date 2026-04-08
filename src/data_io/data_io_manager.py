"""DataIOManager - Strategy Pattern para leitura e escrita de dados."""

import glob
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from src.core.config import ConfigLoader
from src.core.exceptions import DataIOError


class DataIOManager:
    """Gerencia leitura e escrita de dados usando o catalogo de configuracao."""

    def __init__(self, config: ConfigLoader) -> None:
        self._config = config
        self._logger = logging.getLogger("pipeline.data_io_manager")

    def read(self, dataset_id: str) -> pd.DataFrame:
        """Le um dataset pelo seu ID logico do catalogo."""
        entry = self._config.get_catalog_entry(dataset_id)
        fmt = entry["format"]
        full_path = self._config.resolve_input_path(dataset_id)

        self._logger.info(
            "Lendo dataset '%s' (formato=%s) de: %s", dataset_id, fmt, full_path
        )

        if fmt == "json":
            return self._read_json(full_path)
        elif fmt == "csv":
            separator = entry.get("separator", ",")
            return self._read_csv(full_path, separator)
        else:
            raise DataIOError(f"Formato '{fmt}' nao suportado para leitura.")

    def write(self, df: pd.DataFrame, output_id: str) -> str:
        """Escreve um DataFrame no caminho de saida pelo ID logico."""
        entry = self._config.get_output_entry(output_id)
        fmt = entry["format"]
        full_path = self._config.resolve_output_path(output_id)
        separator = entry.get("separator", ",")

        os.makedirs(full_path, exist_ok=True)

        self._logger.info(
            "Escrevendo output '%s' (formato=%s) em: %s", output_id, fmt, full_path
        )

        if fmt == "csv":
            output_file = os.path.join(full_path, f"{output_id}.csv")
            df.to_csv(output_file, sep=separator, index=False, encoding="utf-8")
        elif fmt == "parquet":
            output_file = os.path.join(full_path, f"{output_id}.parquet")
            df.to_parquet(output_file, index=False)
        else:
            raise DataIOError(f"Formato '{fmt}' nao suportado para escrita.")

        self._logger.info("Arquivo salvo em: %s", output_file)
        return output_file

    def _read_json(self, path: str) -> pd.DataFrame:
        """Le um arquivo JSON Lines (suporta .json e .json.gz)."""
        if not os.path.exists(path):
            raise DataIOError(f"Arquivo JSON nao encontrado: {path}")
        try:
            compression = "gzip" if path.endswith(".gz") else "infer"
            df = pd.read_json(path, lines=True, compression=compression)
            self._logger.info("JSON carregado com %d registros.", len(df))
            return df
        except Exception as e:
            raise DataIOError(f"Erro ao ler JSON '{path}': {e}") from e

    def _read_csv(
        self, path: str, separator: str, encoding: Optional[str] = None
    ) -> pd.DataFrame:
        """Le arquivos CSV, suportando diretorio com multiplos arquivos."""
        if encoding is None:
            encoding = "utf-8"

        csv_path = Path(path)

        if csv_path.is_file():
            files = [str(csv_path)]
        elif csv_path.is_dir():
            files = sorted(
                glob.glob(str(csv_path / "*.csv"))
                + glob.glob(str(csv_path / "*.csv.gz"))
            )
        else:
            raise DataIOError(f"Caminho CSV nao encontrado: {path}")

        if not files:
            raise DataIOError(f"Nenhum arquivo CSV encontrado em: {path}")

        self._logger.info("Encontrados %d arquivo(s) CSV.", len(files))

        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, sep=separator, encoding=encoding)
                frames.append(df)
            except Exception as e:
                raise DataIOError(f"Erro ao ler CSV '{f}': {e}") from e

        result = pd.concat(frames, ignore_index=True)
        self._logger.info("CSV carregado com %d registros no total.", len(result))
        return result
