"""Excecoes customizadas para o pipeline."""


class ConfigError(Exception):
    """Erro ao carregar ou validar a configuracao."""

    pass


class DataIOError(Exception):
    """Erro ao ler ou escrever dados."""

    pass
