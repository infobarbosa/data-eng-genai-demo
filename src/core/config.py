"""ConfigLoader - carrega e valida o arquivo config.yaml."""

import os
from pathlib import Path
from typing import Any, Dict

import yaml

from src.core.exceptions import ConfigLoadError


class ConfigLoader:
    """Carrega a configuracao a partir de um arquivo YAML."""

    def __init__(self, config_path: str | None = None) -> None:
        if config_path is None:
            project_root = Path(__file__).resolve().parent.parent.parent
            config_path = str(project_root / "config" / "config.yaml")

        self._config_path = config_path
        self._config: Dict[str, Any] = {}
        self._project_root = Path(config_path).resolve().parent.parent
        self._load()

    def _load(self) -> None:
        """Carrega o arquivo YAML."""
        try:
            with open(self._config_path, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f)
        except FileNotFoundError as e:
            raise ConfigLoadError(
                f"Arquivo de configuracao nao encontrado: {self._config_path}"
            ) from e
        except yaml.YAMLError as e:
            raise ConfigLoadError(
                f"Erro ao fazer parse do YAML: {self._config_path}"
            ) from e

        if not self._config:
            raise ConfigLoadError("Arquivo de configuracao vazio.")

    @property
    def project_root(self) -> Path:
        """Retorna o diretorio raiz do projeto."""
        return self._project_root

    @property
    def config(self) -> Dict[str, Any]:
        """Retorna a configuracao completa."""
        return self._config

    def get_input_base_path(self) -> Path:
        """Retorna o caminho base de entrada."""
        return self._project_root / self._config["data"]["input_base_path"]

    def get_output_base_path(self) -> Path:
        """Retorna o caminho base de saida."""
        return self._project_root / self._config["data"]["output_base_path"]

    def get_catalog_entry(self, dataset_id: str) -> Dict[str, Any]:
        """Retorna a entrada do catalogo para um dataset logico."""
        catalog = self._config.get("catalog", {})
        if dataset_id not in catalog:
            raise ConfigLoadError(f"Dataset '{dataset_id}' nao encontrado no catalogo.")
        return catalog[dataset_id]

    def get_output_entry(self, output_id: str) -> Dict[str, Any]:
        """Retorna a entrada de saida para um output logico."""
        output = self._config.get("output", {})
        if output_id not in output:
            raise ConfigLoadError(
                f"Output '{output_id}' nao encontrado na configuracao."
            )
        return output[output_id]

    def get_pipeline_param(self, param: str) -> Any:
        """Retorna um parametro do pipeline."""
        pipeline = self._config.get("pipeline", {})
        if param not in pipeline:
            raise ConfigLoadError(
                f"Parametro '{param}' nao encontrado na configuracao do pipeline."
            )
        return pipeline[param]

    def get_dataset_info(self, dataset_name: str) -> Dict[str, str]:
        """Retorna informacoes de download de um dataset."""
        datasets = self._config.get("datasets", {})
        if dataset_name not in datasets:
            raise ConfigLoadError(
                f"Dataset '{dataset_name}' nao encontrado em datasets."
            )
        return datasets[dataset_name]

    def resolve_input_path(self, dataset_id: str) -> str:
        """Resolve o caminho fisico completo de um dataset de entrada."""
        entry = self.get_catalog_entry(dataset_id)
        base = self.get_input_base_path()
        return str(base / entry["path"])

    def resolve_output_path(self, output_id: str) -> str:
        """Resolve o caminho fisico completo de um output."""
        entry = self.get_output_entry(output_id)
        base = self.get_output_base_path()
        return str(base / entry["path"])

    def get_env_or_default(self, env_var: str, default: str) -> str:
        """Retorna o valor de uma variavel de ambiente ou um valor padrao."""
        return os.environ.get(env_var, default)
