"""Excecoes customizadas do pipeline."""


class ConfigLoadError(Exception):
    """Erro ao carregar ou validar a configuracao."""

    pass


class DataIOError(Exception):
    """Erro ao ler ou escrever dados."""

    pass


class TransformError(Exception):
    """Erro durante transformacoes de dados."""

    pass
