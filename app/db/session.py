# app/db/session.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager # <--- ДОБАВЛЕНО
from typing import AsyncGenerator         # <--- ДОБАВЛЕНО

from app.core.config import settings # Наши настройки, включая DATABASE_URL

# Асинхронный URL для PostgreSQL
# Ваш config.py уже должен предоставлять settings.DATABASE_URL в формате postgresql+asyncpg://
# благодаря свойству @property. Поэтому здесь дополнительное .replace() не нужно,
# если вы используете обновленный config.py, который я предлагал.
# Если же settings.DATABASE_URL все еще "чистый" postgresql://, то .replace() нужен.
# Для безопасности, я оставлю .replace(), но в идеале config.py должен давать готовую строку.
ASYNC_DATABASE_URL = settings.DATABASE_URL 
if not ASYNC_DATABASE_URL.startswith("postgresql+asyncpg://"):
    ASYNC_DATABASE_URL = ASYNC_DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)


async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False, 
    pool_pre_ping=True
)

# Создаем фабрику асинхронных сессий (остается вашей AsyncSessionFactory)
AsyncSessionFactory = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False, # Рекомендуется для async
    autocommit=False # Рекомендуется для async
)

async def get_async_db() -> AsyncGenerator[AsyncSession, None]: # Изменил возвращаемый тип для соответствия генератору
    """
    Зависимость FastAPI для получения асинхронной сессии базы данных.
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit() 
        except Exception:
            await session.rollback() 
            raise
        finally:
            await session.close()

# --- НАЧАЛО: НОВЫЙ КОНТЕКСТНЫЙ МЕНЕДЖЕР ДЛЯ CELERY ---
@asynccontextmanager
async def get_async_session_context_manager() -> AsyncGenerator[AsyncSession, None]:
    """
    Асинхронный контекстный менеджер для предоставления сессии SQLAlchemy.
    Используется, например, в задачах Celery.
    """
    async with AsyncSessionFactory() as session: # Используем вашу AsyncSessionFactory
        try:
            yield session
            # Коммит не делается здесь автоматически, задача сама должна решить, когда коммитить
            # await session.commit() 
        except Exception:
            # Важно откатить сессию при ошибке, чтобы не оставить транзакцию "висеть"
            await session.rollback()
            raise
        finally:
            # Закрытие сессии также важно
            await session.close()
# --- КОНЕЦ: НОВЫЙ КОНТЕКСТНЫЙ МЕНЕДЖЕР ДЛЯ CELERY ---