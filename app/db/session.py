from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import settings # Наши настройки, включая DATABASE_URL

# Создаем асинхронный движок SQLAlchemy
# settings.DATABASE_URL должен быть асинхронным, т.е. начинаться с postgresql+asyncpg://
# Если settings.DATABASE_URL у вас сейчас postgresql://, его нужно будет обновить.
# Давайте проверим и обновим DATABASE_URL в app/core/config.py

# Асинхронный URL для PostgreSQL
ASYNC_DATABASE_URL = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False, # Установите True для логгирования SQL-запросов (полезно для отладки)
    pool_pre_ping=True # Проверять соединение перед использованием из пула
)

# Создаем фабрику асинхронных сессий
AsyncSessionFactory = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False # Важно для асинхронных сессий, чтобы объекты были доступны после коммита
)

async def get_async_db() -> AsyncSession:
    """
    Зависимость FastAPI для получения асинхронной сессии базы данных.
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit() # Коммит транзакции, если все успешно
        except Exception:
            await session.rollback() # Откат транзакции при ошибке
            raise
        finally:
            await session.close()

# Можно также создать синхронный движок и сессию, если где-то понадобится
# from sqlalchemy import create_engine
# SYNC_DATABASE_URL = settings.DATABASE_URL
# sync_engine = create_engine(SYNC_DATABASE_URL, pool_pre_ping=True)
# SyncSessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)
# def get_sync_db():
#     db = SyncSessionFactory()
#     try:
#         yield db
#     finally:
#         db.close()