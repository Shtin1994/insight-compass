import asyncio
import os
import time
import traceback
from datetime import timezone # Для установки timezone в datetime объектах

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from telethon.errors import FloodWaitError # Импортируем ошибку FloodWaitError
from telethon.tl.types import Message # Для аннотации типа

from app.celery_app import celery_instance
from app.core.config import settings
from app.db.session import AsyncSessionFactory
from app.models.telegram_data import Channel, Post # Добавляем модель Post
from telethon import TelegramClient


# --- Существующие тестовые задачи (оставляем без изменений) ---
@celery_instance.task(name="add_numbers")
def add(x: int, y: int) -> int:
    print(f"Celery таск 'add_numbers': Получены числа {x} и {y}")
    time.sleep(5) 
    result = x + y
    print(f"Celery таск 'add_numbers': Результат сложения {x} + {y} = {result}")
    return result

@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str):
    print(f"Сообщение от simple_debug_task: {message}")
    return f"Сообщение '{message}' обработано Celery!"
# --- Конец существующих тестовых задач ---


@celery_instance.task(name="collect_telegram_data")
def collect_telegram_data_task():
    print("Запущен таск collect_telegram_data_task (сбор постов)...")

    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    target_channels_list = settings.TARGET_TELEGRAM_CHANNELS

    if not all([api_id_val, api_hash_val, phone_number_val]):
        print("Ошибка: Необходимые учетные данные Telegram не настроены.")
        return "Ошибка конфигурации Telegram"

    session_file_path_in_container = "/app/my_telegram_session"
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic():
        tg_client = None
        db_session: AsyncSession | None = None
        local_async_engine = None

        try:
            from sqlalchemy.ext.asyncio import create_async_engine # Импорт здесь, чтобы не было циклов
            from sqlalchemy.orm import sessionmaker

            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(
                bind=local_async_engine, class_=AsyncSession, expire_on_commit=False
            )

            tg_client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val)
            print(f"Подключение к Telegram, сессия: {session_file_path_in_container}.session")
            await tg_client.connect()

            if not await tg_client.is_user_authorized():
                # ... (логика ошибки авторизации) ...
                return "Ошибка авторизации Telegram в Celery"
            
            me = await tg_client.get_me()
            print(f"Celery таск успешно подключен как: {me.first_name} (@{me.username or ''})")

            async with LocalAsyncSessionFactory() as db_session:
                for channel_identifier in target_channels_list:
                    print(f"\nОбработка канала: {channel_identifier}")
                    current_channel_object_in_db: Channel | None = None # Для хранения объекта канала из БД
                    try:
                        channel_entity = await tg_client.get_entity(channel_identifier)
                        print(f"  Получена информация из Telegram: ID={channel_entity.id}, Title='{channel_entity.title}', Username='{getattr(channel_entity, 'username', None)}'")

                        stmt = select(Channel).where(Channel.id == channel_entity.id)
                        result = await db_session.execute(stmt)
                        current_channel_object_in_db = result.scalar_one_or_none()

                        if current_channel_object_in_db:
                            print(f"  Канал {channel_entity.title} уже существует в БД. ID: {current_channel_object_in_db.id}")
                        else:
                            print(f"  Канал {channel_entity.title} не найден в БД. Добавляем...")
                            new_channel_db_obj = Channel(
                                id=channel_entity.id,
                                username=getattr(channel_entity, 'username', None),
                                title=channel_entity.title,
                                description=getattr(channel_entity, 'about', None),
                                is_active=True
                            )
                            db_session.add(new_channel_db_obj)
                            current_channel_object_in_db = new_channel_db_obj # Используем новый объект
                            print(f"  Канал {new_channel_db_obj.title} добавлен в сессию БД с ID: {new_channel_db_obj.id}")
                        
                        # --- СБОР ПОСТОВ ---
                        if current_channel_object_in_db:
                            print(f"  Начинаем сбор постов для канала '{current_channel_object_in_db.title}'...")
                            offset_id_to_use = current_channel_object_in_db.last_processed_post_id or 0
                            # Если last_processed_post_id = 0 (или None), то offset_id=0 заберет самые новые.
                            # Telethon: offset_id - ID сообщения, с которого начинать (исключая его).
                            # Для получения сообщений *после* offset_id, используем reverse=True.
                            # Либо можно использовать min_id, чтобы получить сообщения с ID > min_id.
                            # Проще всего для начала: если offset_id_to_use > 0, то используем min_id.
                            # Если offset_id_to_use == 0, то берем последние N (например, 20).
                            
                            limit_posts = 20 # Сколько постов забирать за раз (для новых каналов или если давно не обновляли)
                            collected_posts_count = 0
                            max_telegram_post_id_in_batch = offset_id_to_use # Инициализируем

                            # Используем min_id, чтобы получить сообщения новее, чем last_processed_post_id
                            # reverse=False (по умолчанию) - от новых к старым
                            async for message in tg_client.iter_messages(channel_entity, limit=limit_posts, min_id=offset_id_to_use):
                                message: Message # Аннотация типа
                                if not message.text and not message.media: # Пропускаем служебные сообщения или пустые
                                    continue

                                # Проверяем, есть ли такой пост уже в БД
                                stmt_post = select(Post).where(Post.telegram_post_id == message.id, Post.channel_id == current_channel_object_in_db.id)
                                result_post = await db_session.execute(stmt_post)
                                db_post = result_post.scalar_one_or_none()

                                if db_post:
                                    # print(f"    Пост ID {message.id} уже существует в БД. Пропускаем.")
                                    # Обновляем max_telegram_post_id_in_batch, если этот пост новее
                                    if message.id > max_telegram_post_id_in_batch:
                                        max_telegram_post_id_in_batch = message.id
                                    continue 

                                print(f"    Новый пост ID {message.id}. Текст: '{message.text[:50] if message.text else '[Медиа]'}'...")
                                
                                # Формируем ссылку на пост (если username канала есть)
                                post_link = f"https://t.me/{current_channel_object_in_db.username}/{message.id}" if current_channel_object_in_db.username else f"https://t.me/c/{current_channel_object_in_db.id}/{message.id}"
                                
                                new_post_db_obj = Post(
                                    telegram_post_id=message.id,
                                    channel_id=current_channel_object_in_db.id,
                                    link=post_link,
                                    text_content=message.text,
                                    views_count=message.views,
                                    # comments_count - пока не трогаем, или нужно делать отдельный запрос
                                    posted_at=message.date.replace(tzinfo=timezone.utc) # Убедимся, что есть таймзона
                                )
                                db_session.add(new_post_db_obj)
                                collected_posts_count += 1
                                if message.id > max_telegram_post_id_in_batch:
                                    max_telegram_post_id_in_batch = message.id
                            
                            if collected_posts_count > 0:
                                print(f"    Собрано и добавлено в сессию {collected_posts_count} новых постов.")
                                # Обновляем last_processed_post_id для канала
                                current_channel_object_in_db.last_processed_post_id = max_telegram_post_id_in_batch
                                db_session.add(current_channel_object_in_db) # SQLAlchemy отследит изменения
                                print(f"    Обновлен last_processed_post_id для канала '{current_channel_object_in_db.title}' на {max_telegram_post_id_in_batch}.")
                            else:
                                print(f"    Новых постов для канала '{current_channel_object_in_db.title}' не найдено (min_id={offset_id_to_use}).")

                    except FloodWaitError as fwe:
                        print(f"  !!! FloodWaitError для канала {channel_identifier}: ждем {fwe.seconds} секунд.")
                        await asyncio.sleep(fwe.seconds + 5) # Ждем указанное время + небольшой запас
                    except ValueError as ve:
                        print(f"  Ошибка (ValueError) при обработке канала {channel_identifier}: {ve}")
                    except Exception as e_channel:
                        print(f"  Неожиданная ошибка при обработке канала {channel_identifier}: {e_channel}")
                        traceback.print_exc()
                
                await db_session.commit()
                print("\nВсе изменения (каналы и посты) сохранены в БД.")
            return "Сбор данных (с постами) завершен."

        # ... (блоки except и finally для tg_client и local_async_engine) ...
        except Exception as e_main:
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
            if local_async_engine:
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