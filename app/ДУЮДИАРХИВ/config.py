# app/core/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, timezone
import os
from typing import List, Optional

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

    DATABASE_URL_FOR_ALEMBIC: Optional[str] = None 

    @property
    def DATABASE_URL(self) -> str:
        base_db_url: str
        if self.DATABASE_URL_FOR_ALEMBIC:
            base_db_url = self.DATABASE_URL_FOR_ALEMBIC.replace("+asyncpg", "")
        else:
            base_db_url = f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        
        if not base_db_url.startswith("postgresql+asyncpg://"):
            if base_db_url.startswith("postgresql://"):
                return base_db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            else: 
                return f"postgresql+asyncpg://{base_db_url}" 
        return base_db_url


    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379

    TELEGRAM_API_ID: Optional[int] = None
    TELEGRAM_API_HASH: Optional[str] = None
    TELEGRAM_PHONE_NUMBER_FOR_LOGIN: Optional[str] = None

    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_TARGET_CHAT_ID: Optional[str] = None

    OPENAI_API_KEY: Optional[str] = None
    OPENAI_DEFAULT_MODEL: Optional[str] = "gpt-3.5-turbo"
    OPENAI_DEFAULT_MODEL_FOR_TASKS: Optional[str] = "gpt-3.5-turbo-1106" 
    OPENAI_API_URL: Optional[str] = "https://api.openai.com/v1/chat/completions"
    OPENAI_TIMEOUT_SECONDS: Optional[float] = 60.0
    LLM_MAX_PROMPT_LENGTH: Optional[int] = 3800 
    
    TARGET_TELEGRAM_CHANNELS_LEGACY: List[str] = []

    INITIAL_POST_FETCH_START_DATE_STR: Optional[str] = None

    @property
    def INITIAL_POST_FETCH_START_DATETIME(self) -> Optional[datetime]:
        if self.INITIAL_POST_FETCH_START_DATE_STR:
            try:
                return datetime.strptime(self.INITIAL_POST_FETCH_START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"ПРЕДУПРЕЖДЕНИЕ: Неверный формат INITIAL_POST_FETCH_START_DATE_STR: '{self.INITIAL_POST_FETCH_START_DATE_STR}'. Ожидается ГГГГ-ММ-ДД. Будет использовано None.")
                return None
        return None

    POST_FETCH_LIMIT: int = 25
    COMMENT_FETCH_LIMIT: int = 200
    # --- НАЧАЛО: Добавлена новая настройка ---
    AI_ANALYSIS_BATCH_SIZE: int = 100 # Размер пачки для постановки комментариев на AI-анализ
    # --- КОНЕЦ: Добавлена новая настройка ---

    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'),
        env_file_encoding='utf-8',
        extra='ignore'
    )

settings = Settings()

# Для отладки при импорте модуля config, если нужно
# print(f"--- Config Loaded ---")
# print(f"PROJECT_NAME: {settings.PROJECT_NAME}")
# print(f"DATABASE_URL (for async app): {settings.DATABASE_URL}")
# if settings.DATABASE_URL_FOR_ALEMBIC:
#    print(f"DATABASE_URL_FOR_ALEMBIC: {settings.DATABASE_URL_FOR_ALEMBIC}")
# print(f"OPENAI_API_KEY is set: {'Yes' if settings.OPENAI_API_KEY else 'No'}")
# print(f"OPENAI_DEFAULT_MODEL_FOR_TASKS: {settings.OPENAI_DEFAULT_MODEL_FOR_TASKS}")
# print(f"AI_ANALYSIS_BATCH_SIZE: {settings.AI_ANALYSIS_BATCH_SIZE}") # <--- Можно добавить для отладки
# print(f"--- End Config Loaded ---")