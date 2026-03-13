class ConfigNotFoundError(Exception):
    """Raised when the configuration file cannot be found."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Configuration file not found: {path}")


class DataSourceNotFoundError(Exception):
    """Raised when a logical data source ID is not found in the catalog."""

    def __init__(self, source_id: str) -> None:
        self.source_id = source_id
        super().__init__(
            f"Data source '{source_id}' not found in the catalog."
        )
