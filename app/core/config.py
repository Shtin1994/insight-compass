from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    # FastAPI App settings
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    PROJECT_NAME: str = "Инсайт-Компас" # <--- ЭТО ПОЛЕ БЫЛО ПРОПУЩЕНО
    API_V1_STR: str = "/api/v1"          # <--- И ЭТО ТОЖЕ, НА ВСЯКИЙ СЛУЧАЙ

    # PostgreSQL settings
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str = "db"
    POSTGRES_PORT: int = 5432

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # Redis settings
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379

    # Telegram API Credentials (пока опциональные, чтобы не падать если их нет в .env)
    TELEGRAM_API_ID: int | None = None
    TELEGRAM_API_HASH: str | None = None
    TELEGRAM_BOT_TOKEN: str | None = None
    TELEGRAM_PHONE_NUMBER_FOR_LOGIN: str | None = None

    # OpenAI API Key (пока опциональный)
    OPENAI_API_KEY: str | None = None

    model_config = SettingsConfigDict(env_file_encoding='utf-8', extra='ignore', env_file='/app/.env')

settings = Settings()

# Для отладки, можно будет убрать позже
if __name__ == "__main__":
    print("Загруженные настройки:")
    if hasattr(settings, 'PROJECT_NAME'): # Проверка на существование атрибута
        print(f"  Имя проекта: {settings.PROJECT_NAME}")
    if hasattr(settings, 'APP_HOST'):
        print(f"  Хост приложения: {settings.APP_HOST}")
    if hasattr(settings, 'APP_PORT'):
        print(f"  Порт приложения: {settings.APP_PORT}")
    print(f"  URL базы данных: {settings.DATABASE_URL}")
    print(f"  Хост Redis: {settings.REDIS_HOST}")
    print(f"  Порт Redis: {settings.REDIS_PORT}")
    # Проверка загрузки переменных из .env
    print(f"  Пользователь PG (из env): {os.getenv('POSTGRES_USER')}")
    print(f"  Пароль PG (из env): {os.getenv('POSTGRES_PASSWORD')}")
    print(f"  Имя БД PG (из env): {os.getenv('POSTGRES_DB')}")
    if settings.TELEGRAM_API_ID:
        print(f"  TELEGRAM_API_ID: {settings.TELEGRAM_API_ID}")
    if settings.OPENAI_API_KEY:
        print(f"  OPENAI_API_KEY: {settings.OPENAI_API_KEY}")