"""Application settings for Hydromancer ingestion and wallet labeling."""

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for the market-wide allFills WebSocket consumer."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    hydromancer_api_key: str = Field(alias="HYDROMANCER_API_KEY", min_length=1)
    hydromancer_coins: str = Field(default="SOL,BTC,ETH", alias="HYDROMANCER_COINS")
    pg_dsn: str = Field(alias="PG_DSN")
    label_interval_seconds: int = Field(default=3600, alias="LABEL_INTERVAL_SECONDS", gt=0)
    l2_volatility_lookback: int = Field(default=1000, alias="L2_VOLATILITY_LOOKBACK", ge=20)
    l2_volatility_min_samples: int = Field(
        default=20, alias="L2_VOLATILITY_MIN_SAMPLES", ge=2
    )
    l2_retention_hours: int = Field(default=168, alias="L2_RETENTION_HOURS", gt=0)
    l4_retention_hours: int = Field(default=24, alias="L4_RETENTION_HOURS", gt=0)
    reconnect_delay_seconds: float = Field(
        default=1.0, alias="HYDROMANCER_RECONNECT_DELAY_SECONDS", gt=0
    )
    replay_status_interval_seconds: float = Field(
        default=5.0, alias="HYDROMANCER_REPLAY_STATUS_INTERVAL_SECONDS", gt=0
    )
    replay_stall_timeout_seconds: float = Field(
        default=30.0, alias="HYDROMANCER_REPLAY_STALL_TIMEOUT_SECONDS", gt=0
    )

    @field_validator("hydromancer_coins")
    @classmethod
    def nonempty_coins(cls, value: str) -> str:
        """Validate that a comma-separated coin setting contains a coin.

        Args:
            value (str): Raw comma-separated coin identifiers.

        Returns:
            str: The unchanged setting value.

        Raises:
            ValueError: If the value contains no non-whitespace coin identifiers.

        """
        if not any(coin.strip() for coin in value.split(",")):
            raise ValueError("HYDROMANCER_COINS must contain at least one coin")
        return value

    @property
    def coins(self) -> list[str]:
        """Return configured case-sensitive protocol coin identifiers.

        Returns:
            list[str]: Trimmed, nonempty coin identifiers in configured order.

        """
        # Coin names are protocol identifiers and deliberately case-sensitive.
        return [coin.strip() for coin in self.hydromancer_coins.split(",") if coin.strip()]

    @property
    def symbols(self) -> list[str]:
        """Return case-preserving coins under the legacy symbols name.

        Unlike the retired REST path, this compatibility property does not
        normalize protocol identifier casing.

        Returns:
            list[str]: Case-preserving protocol coin identifiers.

        """
        return self.coins


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache the process-wide application settings.

    Returns:
        Settings: Validated settings loaded from environment variables and ``.env``.

    Raises:
        pydantic.ValidationError: If required settings are missing or invalid.

    """
    return Settings()  # type: ignore[call-arg]
