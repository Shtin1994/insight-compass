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
                # На случай, если кто-то передаст URL без схемы вообще
                return f"postgresql+asyncpg://{base_db_url}" 
        return base_db_url


    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379

    TELEGRAM_API_ID: Optional[int] = None
    TELEGRAM_API_HASH: Optional[str] = None
    TELEGRAM_PHONE_NUMBER_FOR_LOGIN: Optional[str] = None

    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_TARGET_CHAT_ID: Optional[str] = None # Может быть int или str, лучше str для гибкости

    OPENAI_API_KEY: Optional[str] = None
    OPENAI_DEFAULT_MODEL: Optional[str] = "gpt-3.5-turbo"
    OPENAI_DEFAULT_MODEL_FOR_TASKS: Optional[str] = "gpt-3.5-turbo-1106" 
    OPENAI_API_URL: Optional[str] = "https://api.openai.com/v1/chat/completions"
    OPENAI_TIMEOUT_SECONDS: Optional[float] = 60.0
    LLM_MAX_PROMPT_LENGTH: Optional[int] = 3800 
    
    # Эта настройка, возможно, уже не используется, если каналы управляются через БД
    TARGET_TELEGRAM_CHANNELS_LEGACY: List[str] = [] 

    INITIAL_POST_FETCH_START_DATE_STR: Optional[str] = None

    @property
    def INITIAL_POST_FETCH_START_DATETIME(self) -> Optional[datetime]:
        if self.INITIAL_POST_FETCH_START_DATE_STR:
            try:
                # Убедимся, что дата парсится как naive, а потом добавляется UTC
                dt_naive = datetime.strptime(self.INITIAL_POST_FETCH_START_DATE_STR, "%Y-%m-%d")
                return dt_naive.replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"ПРЕДУПРЕЖДЕНИЕ: Неверный формат INITIAL_POST_FETCH_START_DATE_STR: '{self.INITIAL_POST_FETCH_START_DATE_STR}'. Ожидается ГГГГ-ММ-ДД. Будет использовано None.")
                return None
        return None

    POST_FETCH_LIMIT: int = 25 # Лимит постов при обычном инкрементальном сборе
    COMMENT_FETCH_LIMIT: int = 200 # Лимит комментариев при обычном сборе для поста

    # Настройки для пакетного AI-анализа (Кнопка 3 и далее)
    AI_ANALYSIS_BATCH_SIZE: int = 100      # Общий лимит для постановки комментариев на детальный AI-анализ (используется в advanced_data_refresh)
    POST_ANALYSIS_BATCH_SIZE: int = 100    # Для задачи analyze_posts_sentiment_task (тональность постов)
    POST_SUMMARY_BATCH_SIZE: int = 25      # Для задачи summarize_top_posts_task (суммаризация постов)
    COMMENT_ENQUEUE_BATCH_SIZE: int = 1000  # Для задачи enqueue_comments_for_ai_feature_analysis_task (постановка комментов в очередь)


    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'), # Путь к .env относительно текущего файла
        env_file_encoding='utf-8',
        extra='ignore' # Игнорировать лишние переменные в .env
    )

settings = Settings()

# Для отладки при импорте модуля config, если нужно
# print(f"--- Config Loaded ---")
# print(f"PROJECT_NAME: {settings.PROJECT_NAME}")
# print(f"DATABASE_URL (for async app): {settings.DATABASE_URL}")
# print(f"POST_ANALYSIS_BATCH_SIZE: {settings.POST_ANALYSIS_BATCH_SIZE}")
# print(f"POST_SUMMARY_BATCH_SIZE: {settings.POST_SUMMARY_BATCH_SIZE}")
# print(f"COMMENT_ENQUEUE_BATCH_SIZE: {settings.COMMENT_ENQUEUE_BATCH_SIZE}")
# print(f"--- End Config Loaded ---")