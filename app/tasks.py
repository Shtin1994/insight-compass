import asyncio
import os
import time
import traceback

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession # Добавляем create_async_engine
from sqlalchemy.orm import sessionmaker # Добавляем sessionmaker
from sqlalchemy.future import select

from app.celery_app import celery_instance
from app.core.config import settings
# Убираем импорт AsyncSessionFactory отсюда, будем создавать ее внутри таска
# from app.db.session import AsyncSessionFactory 
from app.models.telegram_data import Channel
from telethon import TelegramClient

# --- Существующие тестовые задачи (оставляем без изменений) ---
# ... add ...
# ... simple_debug_task ...
# --- Конец существующих тестовых задач ---

@celery_instance.task(name="collect_telegram_data")
def collect_telegram_data_task():
    print("Запущен таск collect_telegram_data_task (новый engine/session factory per task)...")

    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    target_channels_list = settings.TARGET_TELEGRAM_CHANNELS

    if not all([api_id_val, api_hash_val, phone_number_val]):
        print("Ошибка: Необходимые учетные данные Telegram не настроены.")
        return "Ошибка конфигурации Telegram"

    session_file_path_in_container = "/app/my_telegram_session"
    
    # Формируем АСИНХРОННЫЙ URL для PostgreSQL внутри таска
    # settings.DATABASE_URL у нас синхронный
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic():
        tg_client = None
        db_session: AsyncSession | None = None
        local_async_engine = None # Локальный движок для этого таска

        try:
            # Создаем локальный async_engine для этого вызова таска
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            
            # Создаем локальную фабрику сессий на основе локального движка
            LocalAsyncSessionFactory = sessionmaker(
                bind=local_async_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            tg_client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val)
            print(f"Подключение к Telegram, сессия: {session_file_path_in_container}.session")
            await tg_client.connect()

            if not await tg_client.is_user_authorized():
                print(f"ОШИБКА: Пользователь не авторизован! Сессия: {session_file_path_in_container}.session")
                # ... (отладочная печать файла сессии) ...
                return "Ошибка авторизации Telegram в Celery"
            
            me = await tg_client.get_me()
            print(f"Celery таск успешно подключен как: {me.first_name} (@{me.username or ''})")

            async with LocalAsyncSessionFactory() as db_session: # Используем локальную фабрику
                for channel_identifier in target_channels_list:
                    # ... (логика обработки каналов и БД остается такой же) ...
                    print(f"\nОбработка канала: {channel_identifier}")
                    try:
                        channel_entity = await tg_client.get_entity(channel_identifier)
                        print(f"  Получена информация из Telegram: ID={channel_entity.id}, Title='{channel_entity.title}', Username='{getattr(channel_entity, 'username', None)}'")

                        stmt = select(Channel).where(Channel.id == channel_entity.id)
                        result = await db_session.execute(stmt)
                        db_channel = result.scalar_one_or_none()

                        if db_channel:
                            print(f"  Канал {channel_entity.title} уже существует в БД. ID: {db_channel.id}")
                        else:
                            print(f"  Канал {channel_entity.title} не найден в БД. Добавляем...")
                            new_channel = Channel(
                                id=channel_entity.id,
                                username=getattr(channel_entity, 'username', None),
                                title=channel_entity.title,
                                description=getattr(channel_entity, 'about', None),
                                is_active=True
                            )
                            db_session.add(new_channel)
                            print(f"  Канал {new_channel.title} добавлен в сессию БД с ID: {new_channel.id}")
                        
                    except ValueError as ve:
                        print(f"  Ошибка (ValueError) при получении информации о канале {channel_identifier}: {ve}")
                    except Exception as e_channel:
                        print(f"  Неожиданная ошибка при обработке канала {channel_identifier}: {e_channel}")
                        traceback.print_exc()
                
                await db_session.commit()
                print("\nВсе изменения для каналов сохранены в БД.")
            return "Сбор данных (с сохранением каналов) завершен."

        except Exception as e_main:
            # ... (обработка ошибок и rollback) ...
            print(f"Главная ошибка в асинхронной логике Celery таска: {e_main}")
            traceback.print_exc()
            if db_session and db_session.is_active: 
                try:
                    await db_session.rollback()
                    print("Транзакция БД отменена из-за ошибки.")
                except Exception as e_rollback:
                    print(f"Ошибка при откате транзакции БД: {e_rollback}")
            return f"Ошибка в Celery таске: {str(e_main)}"
        finally:
            if tg_client and tg_client.is_connected():
                print("Отключение от Telegram в Celery таске.")
                await tg_client.disconnect()
            if local_async_engine: # Закрываем пул соединений движка
                print("Закрытие локального async_engine.")
                await local_async_engine.dispose()

    # ... (логика запуска asyncio.run() остается такой же) ...
    result_message = "Не удалось выполнить asyncio.run"
    try:
        result_message = asyncio.run(_async_main_logic())
    except Exception as e_outer_run:
        print(f"Критическая ошибка при запуске asyncio.run в Celery таске: {e_outer_run}")
        traceback.print_exc()
        result_message = f"Ошибка запуска asyncio: {str(e_outer_run)}"
    print(f"Результат выполнения collect_telegram_data_task: {result_message}")
    return result_message