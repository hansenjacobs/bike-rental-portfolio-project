from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    db_host: str = 'localhost'
    db_name: str = 'citi_bike'
    db_password: str | None
    db_port: int = 5432
    db_user: str = 'admin'

    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')
