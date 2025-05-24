import asyncio
import os
import time 
import traceback
from datetime import timezone

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from telethon.errors import FloodWaitError
from telethon.tl.types import Message

from app.celery_app import celery_instance
from app.core.config import settings
from app.models.telegram_data import Channel, Post
from telethon import TelegramClient


# --- Существующие тестовые задачи ---
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
    print("Запущен таск collect_telegram_data_task (УЛУЧШЕННАЯ ПАГИНАЦІЯ ПОСТІВ)...")

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
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(
                bind=local_async_engine, class_=AsyncSession, expire_on_commit=False
            )

            tg_client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val)
            print(f"Подключение к Telegram, сессия: {session_file_path_in_container}.session")
            await tg_client.connect()

            if not await tg_client.is_user_authorized():
                print(f"ОШИБКА: Пользователь не авторизован! Сессия: {session_file_path_in_container}.session")
                # ... (отладочная печать файла сессии, если нужно) ...
                return "Ошибка авторизации Telegram в Celery"
            
            me = await tg_client.get_me()
            print(f"Celery таск успешно подключен как: {me.first_name} (@{me.username or ''})")

            async with LocalAsyncSessionFactory() as db_session:
                for channel_identifier in target_channels_list:
                    print(f"\nОбработка канала: {channel_identifier}")
                    current_channel_object_in_db: Channel | None = None
                    newly_added_post_ids_in_batch = [] 

                    try:
                        channel_entity = await tg_client.get_entity(channel_identifier)
                        print(f"  Получена информация из Telegram: ID={channel_entity.id}, Title='{channel_entity.title}', Username='{getattr(channel_entity, 'username', None)}'")

                        stmt = select(Channel).where(Channel.id == channel_entity.id)
                        result = await db_session.execute(stmt)
                        current_channel_object_in_db = result.scalar_one_or_none()

                        if not current_channel_object_in_db:
                            print(f"  Канал {channel_entity.title} не найден в БД. Добавляем...")
                            new_channel_db_obj = Channel(
                                id=channel_entity.id,
                                username=getattr(channel_entity, 'username', None),
                                title=channel_entity.title,
                                description=getattr(channel_entity, 'about', None),
                                is_active=True
                            )
                            db_session.add(new_channel_db_obj)
                            await db_session.flush() 
                            current_channel_object_in_db = new_channel_db_obj
                            print(f"  Канал {new_channel_db_obj.title} добавлен в сессию БД с ID: {new_channel_db_obj.id}")
                        else:
                            print(f"  Канал {channel_entity.title} уже существует в БД. ID: {current_channel_object_in_db.id}")
                        
                        # --- УЛУЧШЕННЫЙ СБОР ПОСТОВ (ПАГИНАЦИЯ) ---
                        if current_channel_object_in_db:
                            print(f"  Начинаем сбор ВСЕХ НОВЫХ постов для канала '{current_channel_object_in_db.title}'...")
                            
                            # min_id - это ID последнего обработанного поста. Собираем все, что новее.
                            # Этот ID не меняется внутри цикла сбора постов для текущего канала.
                            initial_min_id_for_channel = current_channel_object_in_db.last_processed_post_id or 0
                            
                            # Переменная для отслеживания самого нового ID поста, который мы увидели в этом запуске
                            # Инициализируем ее текущим значением из БД.
                            latest_post_id_seen_this_run = initial_min_id_for_channel

                            # Лимит сообщений, запрашиваемых за один вызов API (пачка)
                            # Telethon сам будет делать несколько запросов, если limit в iter_messages будет больше,
                            # но лучше управлять этим явно для контроля.
                            TELETHON_API_CALL_LIMIT = 100 # Сколько за раз просить у Telegram API
                            
                            # Общий лимит постов, которые мы хотим собрать за ОДИН ЗАПУСК ТАСКА для этого канала
                            # Это для предотвращения слишком долгих операций, если канал очень активный
                            # и мы давно его не обновляли.
                            MAX_POSTS_PER_TASK_RUN_FOR_CHANNEL = 500 # Например, не более 500 постов за раз

                            total_collected_for_channel_this_run = 0
                            
                            # offset_id для iter_messages. Начинаем с 0 (самые новые).
                            # При каждом успешном получении пачки, будем обновлять offset_id на ID последнего сообщения в пачке.
                            # iter_messages с offset_id и reverse=False (по умолчанию) идет от offset_id к более старым.
                            # Нам нужно идти от min_id к более новым.
                            # Поэтому используем min_id и итерируемся, пока есть сообщения.
                            # `iter_messages` будет возвращать сообщения от новых к старым, если min_id указан.

                            print(f"  Запрашиваем посты с ID > {initial_min_id_for_channel}. Общий лимит на запуск: {MAX_POSTS_PER_TASK_RUN_FOR_CHANNEL}")
                            
                            temp_posts_buffer = [] # Временный буфер для постов перед добавлением в сессию

                            # Собираем посты пачками, пока не достигнем лимита или пока не закончатся новые посты
                            # Мы будем использовать iter_messages, он сам управляет пачками под капотом, если limit большой
                            # Но мы все равно ограничиваем MAX_POSTS_PER_TASK_RUN_FOR_CHANNEL
                            async for message in tg_client.iter_messages(
                                channel_entity,
                                limit=MAX_POSTS_PER_TASK_RUN_FOR_CHANNEL, # Общий лимит на эту итерацию
                                min_id=initial_min_id_for_channel # Только сообщения новее этого ID
                                # Сообщения будут идти от более новых к более старым (среди тех, что > min_id)
                            ):
                                message: Message
                                if not message.text and not message.media:
                                    continue
                                
                                if message.id > latest_post_id_seen_this_run:
                                    latest_post_id_seen_this_run = message.id

                                # Проверка на дубликат (на случай, если min_id не сработал идеально или параллельный запуск)
                                stmt_post_check = select(Post.id).where(Post.telegram_post_id == message.id, Post.channel_id == current_channel_object_in_db.id)
                                result_post_check = await db_session.execute(stmt_post_check)
                                if result_post_check.scalar_one_or_none() is not None:
                                    continue

                                print(f"    Найден новый пост ID {message.id}. Текст: '{message.text[:30] if message.text else '[Медиа]'}'...")
                                post_link = f"https://t.me/{current_channel_object_in_db.username}/{message.id}" if current_channel_object_in_db.username else f"https://t.me/c/{current_channel_object_in_db.id}/{message.id}"
                                
                                new_post_obj = Post(
                                    telegram_post_id=message.id,
                                    channel_id=current_channel_object_in_db.id,
                                    link=post_link,
                                    text_content=message.text,
                                    views_count=message.views,
                                    posted_at=message.date.replace(tzinfo=timezone.utc)
                                )
                                temp_posts_buffer.append(new_post_obj)
                                newly_added_post_ids_in_batch.append(message.id)
                                total_collected_for_channel_this_run += 1
                            
                            if temp_posts_buffer:
                                db_session.add_all(temp_posts_buffer)
                                print(f"    Добавлено в сессию {total_collected_for_channel_this_run} новых постов для канала '{current_channel_object_in_db.title}'.")
                            
                            if latest_post_id_seen_this_run > (current_channel_object_in_db.last_processed_post_id or 0):
                                current_channel_object_in_db.last_processed_post_id = latest_post_id_seen_this_run
                                db_session.add(current_channel_object_in_db)
                                print(f"    Обновлен last_processed_post_id для канала '{current_channel_object_in_db.title}' на {latest_post_id_seen_this_run}.")
                            elif total_collected_for_channel_this_run == 0:
                                print(f"    Новых постов для канала '{current_channel_object_in_db.title}' не найдено (проверено ID > {initial_min_id_for_channel}).")
                        
                        # TODO: ЗДЕСЬ БУДЕТ СБОР КОММЕНТАРИЕВ для newly_added_post_ids_in_batch

                    except FloodWaitError as fwe:
                        print(f"  !!! FloodWaitError для канала {channel_identifier}: ждем {fwe.seconds} секунд.")
                        await asyncio.sleep(fwe.seconds + 5)
                    except ValueError as ve:
                        print(f"  Ошибка (ValueError) при обработке канала {channel_identifier}: {ve}")
                    except Exception as e_channel:
                        print(f"  Неожиданная ошибка при обработке канала {channel_identifier}: {e_channel}")
                        traceback.print_exc()
                
                await db_session.commit()
                print("\nВсе изменения (каналы и посты) сохранены в БД.")
            return "Сбор данных (с улучшенной пагинацией постов) завершен."

        # ... (блоки except и finally для tg_client и local_async_engine как в предыдущей версии) ...
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

# В будущем здесь будет настройка Celery Beat для периодического запуска этого таска