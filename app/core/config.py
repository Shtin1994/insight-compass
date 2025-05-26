# --- START OF FILE app/core/config.py (Config for "Last 5 Posts" Test) ---

from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, timezone 
import os

class Settings(BaseSettings):
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    PROJECT_NAME: str = "Инсайт-Компас"
    API_V1_STR: str = "/api/v1"

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str = "db" 
    POSTGRES_PORT: int = 5432

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379

    TELEGRAM_API_ID: int | None = None
    TELEGRAM_API_HASH: str | None = None
    TELEGRAM_PHONE_NUMBER_FOR_LOGIN: str | None = None

    TELEGRAM_BOT_TOKEN: str | None = None
    TELEGRAM_TARGET_CHAT_ID: str | None = None 

    OPENAI_API_KEY: str | None = None
    
    TARGET_TELEGRAM_CHANNELS: list[str] = [
        "nashputwildberries",
        "redmilliard",
        "wbsharks",
        "maxprowb",
        "mpgo_ru",
        "marketplace_hogwarts",
    ]

    INITIAL_POST_FETCH_START_DATE_STR: str | None = None # <--- ИЗМЕНЕНИЕ: None, чтобы брать последние
                                                        # при первом сборе с пустой БД

    @property
    def INITIAL_POST_FETCH_START_DATETIME(self) -> datetime | None:
        if self.INITIAL_POST_FETCH_START_DATE_STR:
            try:
                return datetime.strptime(self.INITIAL_POST_FETCH_START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"ПРЕДУПРЕЖДЕНИЕ: Неверный формат INITIAL_POST_FETCH_START_DATE_STR: '{self.INITIAL_POST_FETCH_START_DATE_STR}'. Ожидается ГГГГ-ММ-ДД.")
                return None
        return None

    POST_FETCH_LIMIT: int = 5   # <--- ИЗМЕНЕНИЕ: Собираем только 5 постов за раз
    COMMENT_FETCH_LIMIT: int = 100 # Оставляем 100 для комментариев (или можно уменьшить до 20-30 для скорости теста)

    model_config = SettingsConfigDict(env_file_encoding='utf-8', extra='ignore', env_file='/app/.env')

settings = Settings()

# --- END OF FILE app/core/config.py (Config for "Last 5 Posts" Test) ---