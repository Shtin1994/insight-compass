# --- START OF FILE app/core/config.py (to replace existing content) ---

from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    # FastAPI App settings
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    PROJECT_NAME: str = "Инсайт-Компас"
    API_V1_STR: str = "/api/v1"

    # PostgreSQL settings
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str = "db" # Имя сервиса из docker-compose.yml
    POSTGRES_PORT: int = 5432

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # Redis settings
    REDIS_HOST: str = "redis" # Имя сервиса из docker-compose.yml
    REDIS_PORT: int = 6379

    # Telegram API Credentials
    # Типы указаны как опциональные (int | None, str | None), чтобы приложение не падало при запуске,
    # если этих переменных нет в .env, но tasks.py будет проверять их наличие перед использованием.
    TELEGRAM_API_ID: int | None = None
    TELEGRAM_API_HASH: str | None = None
    TELEGRAM_BOT_TOKEN: str | None = None # Пока не используется активно в tasks.py, но может понадобиться
    TELEGRAM_PHONE_NUMBER_FOR_LOGIN: str | None = None
    TELEGRAM_TARGET_CHAT_ID: str | None = None # ID чата/пользователя для отправки дайджесто

    # OpenAI API Key
    OPENAI_API_KEY: str | None = None # Пока не используется
    
    # Список Telegram-каналов для мониторинга (для MVP)
    # Можно использовать username (@username), ID канала (число), или joinchat-ссылку (https://t.me/joinchat/...)
    TARGET_TELEGRAM_CHANNELS: list[str] = [
        "durov",                     # Пример username
        "telegram",                  # Пример username
        "breakingmash",              # Пример username
        "marketplace_hogwarts",      # Пример username
        # "https://t.me/joinchat/AAAAAEQb_A0J6L8jX5aW4w" # Пример joinchat ссылки (если нужно)
        # 123456789                   # Пример ID канала (если это публичный канал, известный по ID)
    ]

    # Лимиты для сборщика данных
    POST_FETCH_LIMIT: int = 200  # Лимит постов за один вызов iter_messages для одного канала
    COMMENT_FETCH_LIMIT: int = 100 # Лимит комментариев за один вызов iter_messages для одного поста

    # Конфигурация Pydantic Settings
    # env_file: путь к .env файлу ВНУТРИ контейнера
    # extra='ignore': игнорировать лишние переменные в .env файле
    model_config = SettingsConfigDict(env_file_encoding='utf-8', extra='ignore', env_file='/app/.env')

settings = Settings()

# Блок для отладки при прямом запуске файла config.py (python app/core/config.py)
# В реальной работе приложения этот блок не выполняется.
if __name__ == "__main__":
    print("--- Загруженные настройки (app/core/config.py) ---")
    print(f"  Имя проекта: {settings.PROJECT_NAME}")
    print(f"  Хост приложения: {settings.APP_HOST}")
    print(f"  Порт приложения: {settings.APP_PORT}")
    print(f"  URL базы данных: {settings.DATABASE_URL}")
    print(f"  Хост Redis: {settings.REDIS_HOST}")
    print(f"  Порт Redis: {settings.REDIS_PORT}")
    
    print("\n  --- Переменные из .env (как их видит Pydantic) ---")
    print(f"  POSTGRES_USER: {settings.POSTGRES_USER}")
    print(f"  POSTGRES_PASSWORD: {'********' if settings.POSTGRES_PASSWORD else None}") # Не выводим пароль
    print(f"  POSTGRES_DB: {settings.POSTGRES_DB}")
    
    print(f"\n  --- Telegram API (из .env через Pydantic) ---")
    print(f"  TELEGRAM_API_ID: {settings.TELEGRAM_API_ID}")
    print(f"  TELEGRAM_API_HASH: {'********' if settings.TELEGRAM_API_HASH else None}") # Не выводим хэш
    print(f"  TELEGRAM_PHONE_NUMBER_FOR_LOGIN: {settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN}")
    
    if settings.OPENAI_API_KEY:
        print(f"\n  OPENAI_API_KEY: {'********'}")
    
    print(f"\n  --- Лимиты сборщика ---")
    print(f"  POST_FETCH_LIMIT: {settings.POST_FETCH_LIMIT}")
    print(f"  COMMENT_FETCH_LIMIT: {settings.COMMENT_FETCH_LIMIT}")
    print("--- Конец загруженных настроек ---")

# --- END OF FILE app/core/config.py (to replace existing content) ---