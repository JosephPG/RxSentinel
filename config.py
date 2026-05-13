from pydantic_settings import BaseSettings, SettingsConfigDict

MIGRATE_MODELS: list[str] = ["app.models"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=(".env"), env_file_encoding="utf-8")

    ARANGO_HOST: str = "http://localhost"
    ARANGO_PORT: str = "8529"
    ARANGO_DB: str = "sentinel"
    ARANGO_USERNAME: str = ""
    ARANGO_PASSWORD: str = ""
    KAFKA_SERVERLOG_TOPIC: str = "server-logs"
    KAFKA_SERVERLOG_HOST: str = "localhost:9092"


settings = Settings()
