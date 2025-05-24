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

    # Telegram API Credentials (оставим на потом)
    # TELEGRAM_API_ID: int
    # TELEGRAM_API_HASH: str
    # TELEGRAM_BOT_TOKEN: str

    # OpenAI API Key (оставим на потом)
    # OPENAI_API_KEY: str

    # Для pydantic-settings v2.x
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

# Создаем экземпляр настроек, который будет использоваться в приложении
settings = Settings()

# Для отладки, можно будет убрать позже
if __name__ == "__main__":
    print("Загруженные настройки:")
    print(f"  Имя проекта: {settings.PROJECT_NAME}")
    print(f"  Хост приложения: {settings.APP_HOST}")
    print(f"  Порт приложения: {settings.APP_PORT}")
    print(f"  URL базы данных: {settings.DATABASE_URL}")
    print(f"  Хост Redis: {settings.REDIS_HOST}")
    print(f"  Порт Redis: {settings.REDIS_PORT}")
    # Проверка загрузки переменных из .env (если они там есть)
    # print(f"  Пользователь PG (из env): {os.getenv('POSTGRES_USER')}")
    # print(f"  Пароль PG (из env): {os.getenv('POSTGRES_PASSWORD')}")
    # print(f"  Имя БД PG (из env): {os.getenv('POSTGRES_DB')}")