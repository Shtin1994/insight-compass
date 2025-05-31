# app/core/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, timezone
import os
from typing import List, Optional # Добавил Optional для type hinting

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

    TELEGRAM_API_ID: Optional[int] = None # Сделаем опциональными для гибкости, но для работы они нужны
    TELEGRAM_API_HASH: Optional[str] = None
    TELEGRAM_PHONE_NUMBER_FOR_LOGIN: Optional[str] = None

    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_TARGET_CHAT_ID: Optional[str] = None # Может быть числом или строкой типа "@username"

    OPENAI_API_KEY: Optional[str] = None
    
    # Этот список больше не используется напрямую сборщиком, но может быть полезен для справки
    # или если мы решим его как-то использовать для первоначального заполнения БД.
    # Пока его можно оставить или закомментировать.
    TARGET_TELEGRAM_CHANNELS_LEGACY: List[str] = [
        # "nashputwildberries", # Примеры
        # "mpgo_ru",
        # "redmilliard",
    ]

    # --- НАСТРОЙКИ ДЛЯ ТЕСТИРОВАНИЯ СБОРА ДАННЫХ ---
    # Убедимся, что INITIAL_POST_FETCH_START_DATE_STR = None,
    # чтобы при первом сборе для новых каналов (с last_processed_post_id=NULL)
    # брались последние N постов, а не посты с определенной даты.
    INITIAL_POST_FETCH_START_DATE_STR: Optional[str] = None

    @property
    def INITIAL_POST_FETCH_START_DATETIME(self) -> Optional[datetime]:
        if self.INITIAL_POST_FETCH_START_DATE_STR:
            try:
                # Преобразуем строку в datetime объект с UTC таймзоной
                return datetime.strptime(self.INITIAL_POST_FETCH_START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"ПРЕДУПРЕЖДЕНИЕ: Неверный формат INITIAL_POST_FETCH_START_DATE_STR: '{self.INITIAL_POST_FETCH_START_DATE_STR}'. Ожидается ГГГГ-ММ-ДД. Будет использовано None.")
                return None
        return None

    POST_FETCH_LIMIT: int = 25   # <--- ЛИМИТ ДЛЯ СБОРА 25 ПОСТОВ
    COMMENT_FETCH_LIMIT: int = 200 # <--- УМЕНЬШЕННЫЙ ЛИМИТ ДЛЯ КОММЕНТАРИЕВ

    # Конфигурация Pydantic Settings
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'), # Более надежный путь к .env
        env_file_encoding='utf-8',
        extra='ignore' # Игнорировать лишние переменные в .env файле
    )

# Создаем экземпляр настроек
settings = Settings()

# Вывод для проверки при старте (опционально)
# print(f"DATABASE_URL: {settings.DATABASE_URL}")
# print(f"INITIAL_POST_FETCH_START_DATETIME: {settings.INITIAL_POST_FETCH_START_DATETIME}")
# print(f"POST_FETCH_LIMIT: {settings.POST_FETCH_LIMIT}")