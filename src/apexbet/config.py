"""Central configuration for ApexBet.

All settings come from environment variables (or a local `.env`). Every
default is safe and offline: no AWS account, no paid services, no external
AI API keys are required to run the platform.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

TransportName = Literal["redpanda", "kinesis", "memory"]
StorageBackend = Literal["local", "s3"]
EnrichmentMode = Literal["rules", "local_ml", "llm"]


class Settings(BaseSettings):
    """Runtime configuration assembled from the environment."""

    model_config = SettingsConfigDict(
        env_prefix="APEXBET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- runtime ---
    env: str = "local"
    log_level: str = "INFO"
    log_json: bool = True
    seed: int = 1337

    # --- transport ---
    transport: TransportName = "redpanda"

    kafka_bootstrap: str = "localhost:19092"
    kafka_topic: str = "apexbet.events.v1"
    kafka_dlq_topic: str = "apexbet.events.dlq.v1"
    kafka_group_id: str = "apexbet-consumers"

    # --- aws / kinesis ---
    aws_endpoint_url: str = Field(default="http://localhost:4566", alias="AWS_ENDPOINT_URL")
    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")
    aws_access_key_id: str = Field(default="test", alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(default="test", alias="AWS_SECRET_ACCESS_KEY")
    kinesis_stream: str = "apexbet-events"
    kinesis_shards: int = 2
    use_real_aws: bool = False

    # --- storage ---
    storage_backend: StorageBackend = "local"
    lake_root: str = "./lake"
    s3_bucket: str = "apexbet-lake"
    s3_prefix: str = ""

    # --- api ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_max_body_bytes: int = 1_048_576
    api_rate_limit_per_sec: int = 2000
    api_tokens: str = ""
    api_protected_prefixes: str = "/api/v1/platform"
    # Comma-separated CORS origins for the local dashboard dev server.
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"

    # --- processing ---
    bronze_flush_seconds: float = 5.0
    bronze_flush_records: int = 5000
    dedup_window_seconds: int = 3600
    allowed_lateness_seconds: int = 600
    max_retries: int = 5
    retry_base_seconds: float = 0.5

    # --- analytics ---
    duckdb_path: str = "./lake/analytics.duckdb"

    # --- enrichment ---
    enrichment_mode: EnrichmentMode = "rules"
    llm_enabled: bool = False
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3.2:3b"
    llm_timeout_seconds: float = 20.0

    # --- monitoring ---
    metrics_port: int = 9101

    postgres_dsn: str = "postgresql://apexbet:apexbet@localhost:5432/apexbet"

    @field_validator("api_tokens", "api_protected_prefixes", mode="before")
    @classmethod
    def _coerce_optional_str(cls, v: object) -> str:
        return "" if v is None else str(v)

    @property
    def token_set(self) -> set[str]:
        return {t.strip() for t in self.api_tokens.split(",") if t.strip()}

    @property
    def protected_prefixes(self) -> tuple[str, ...]:
        parts = [p.strip() for p in self.api_protected_prefixes.split(",") if p.strip()]
        return tuple(parts)

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    return Settings()  # type: ignore[call-arg]


def reset_settings_cache() -> None:
    """Clear the cached settings (used by tests)."""
    get_settings.cache_clear()
