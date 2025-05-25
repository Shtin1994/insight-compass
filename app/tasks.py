# --- START OF FILE tasks.py (Corrected) ---

import asyncio
import os
import time
import traceback
from datetime import timezone, datetime # Добавил datetime для created_at/updated_at в моделях

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from telethon.errors import FloodWaitError
from telethon.tl.types import Message, User

from app.celery_app import celery_instance
from app.core.config import settings
from app.models.telegram_data import Channel, Post, Comment
from telethon import TelegramClient

# --- Существующие тестовые задачи (заглушки, если вы их используете) ---
@celery_instance.task(name="add")
def add(x, y):
    print(f"Тестовый таск 'add': {x} + {y}")
    time.sleep(5) # Имитация долгой задачи
    result = x + y
    print(f"Результат 'add': {result}")
    return result

@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str):
    print(f"Тестовый таск 'simple_debug_task' получил сообщение: {message}")
    time.sleep(3)
    return f"Сообщение '{message}' обработано в simple_debug_task"
# --- Конец существующих тестовых задач ---


@celery_instance.task(name="collect_telegram_data", bind=True, max_retries=3, default_retry_delay=60) # bind=True для доступа к self
def collect_telegram_data_task(self): # Добавил self из-за bind=True
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (сбор постов и КОММЕНТАРИЕВ)...")

    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    target_channels_list = settings.TARGET_TELEGRAM_CHANNELS

    if not all([api_id_val, api_hash_val, phone_number_val]):
        error_msg = "Ошибка: Необходимые учетные данные Telegram не настроены (TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE_NUMBER_FOR_LOGIN)."
        print(error_msg)
        # Не используем retry здесь, так как это ошибка конфигурации
        return error_msg # Задача завершится с SUCCESS, но с сообщением об ошибке

    session_file_path_in_container = "/app/my_telegram_session" # Без .session в имени файла для Telethon
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic():
        tg_client = None
        local_async_engine = None
        # db_session управляется через 'async with LocalAsyncSessionFactory() as db_session:'

        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(
                bind=local_async_engine, class_=AsyncSession, expire_on_commit=False
            )

            print(f"Попытка создания TelegramClient с сессией: {session_file_path_in_container}")
            tg_client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val)
            
            print(f"Подключение к Telegram...")
            await tg_client.connect()

            if not await tg_client.is_user_authorized():
                error_auth_msg = f"ОШИБКА: Пользователь не авторизован! Сессия: {session_file_path_in_container}.session. Запустите test_telegram_connection.py."
                print(error_auth_msg)
                # Это критическая ошибка, не требующая retry, а исправления сессии
                # Пробрасываем ошибку, чтобы Celery пометил таск как FAILED
                raise ConnectionRefusedError(error_auth_msg) 
            
            me = await tg_client.get_me()
            print(f"Celery таск успешно подключен как: {me.first_name} (@{me.username or ''}, ID: {me.id})")

            async with LocalAsyncSessionFactory() as db_session:
                for channel_identifier in target_channels_list:
                    print(f"\nОбработка канала: {channel_identifier}")
                    current_channel_db_obj: Channel | None = None
                    newly_added_post_objects_in_session: list[Post] = []

                    try:
                        channel_entity = await tg_client.get_entity(channel_identifier)
                        
                        stmt_channel = select(Channel).where(Channel.id == channel_entity.id)
                        result_channel = await db_session.execute(stmt_channel)
                        current_channel_db_obj = result_channel.scalar_one_or_none()

                        if not current_channel_db_obj:
                            print(f"  Канал '{channel_entity.title}' (ID: {channel_entity.id}) не найден в БД. Добавляем...")
                            new_channel_db_obj_for_add = Channel(
                                id=channel_entity.id,
                                username=getattr(channel_entity, 'username', None),
                                title=channel_entity.title,
                                description=getattr(channel_entity, 'about', None), # 'about' это описание для каналов/чатов
                                is_active=True
                                # created_at и updated_at установятся по умолчанию в БД
                            )
                            db_session.add(new_channel_db_obj_for_add)
                            await db_session.flush() # Получаем ID и другие дефолты из БД, если нужно до коммита
                            current_channel_db_obj = new_channel_db_obj_for_add
                            print(f"  Канал '{new_channel_db_obj_for_add.title}' добавлен в сессию БД с ID: {new_channel_db_obj_for_add.id}")
                        else:
                            print(f"  Канал '{channel_entity.title}' уже существует в БД. ID: {current_channel_db_obj.id}")
                        
                        if current_channel_db_obj:
                            print(f"  Начинаем сбор постов для канала '{current_channel_db_obj.title}'...")
                            initial_min_id_for_channel = current_channel_db_obj.last_processed_post_id or 0
                            latest_post_id_seen_this_run = initial_min_id_for_channel
                            MAX_POSTS_PER_ITERATION = settings.POST_FETCH_LIMIT # Берем из config.py
                            total_collected_for_channel_this_run = 0
                            
                            print(f"  Запрашиваем посты с Telegram ID > {initial_min_id_for_channel}, лимит на итерацию: {MAX_POSTS_PER_ITERATION}")
                            
                            temp_posts_buffer_for_db_add: list[Post] = []

                            # min_id в iter_messages означает "сообщения с ID строго больше этого значения"
                            async for message_tg in tg_client.iter_messages(
                                entity=channel_entity,
                                limit=MAX_POSTS_PER_ITERATION, 
                                min_id=initial_min_id_for_channel,
                                reverse=False # Новые сообщения первыми (не нужно, min_id уже гарантирует это)
                            ):
                                message_tg: Message
                                if not (message_tg.text or message_tg.media): continue # Пропускаем без текста и медиа
                                
                                if message_tg.id > latest_post_id_seen_this_run:
                                    latest_post_id_seen_this_run = message_tg.id

                                # Проверка на существование поста в БД (чтобы избежать дублей, если min_id не сработал идеально или был сброс)
                                stmt_post_check = select(Post.id).where(Post.telegram_post_id == message_tg.id, Post.channel_id == current_channel_db_obj.id)
                                result_post_check = await db_session.execute(stmt_post_check)
                                if result_post_check.scalar_one_or_none() is not None:
                                    # print(f"    Пост ID {message_tg.id} уже существует в БД, пропускаем.")
                                    continue

                                print(f"    Найден новый пост ID {message_tg.id}. Текст: '{message_tg.text[:50].replace(chr(10),' ') if message_tg.text else '[Медиа]'}'...")
                                post_link = f"https://t.me/{current_channel_db_obj.username}/{message_tg.id}" if current_channel_db_obj.username else f"https://t.me/c/{current_channel_db_obj.id}/{message_tg.id}"
                                
                                new_post_db_obj = Post(
                                    telegram_post_id=message_tg.id,
                                    channel_id=current_channel_db_obj.id,
                                    link=post_link,
                                    text_content=message_tg.text,
                                    views_count=message_tg.views,
                                    # comments_count будет обновлен после сбора комментариев
                                    posted_at=message_tg.date.replace(tzinfo=timezone.utc) if message_tg.date else datetime.now(timezone.utc)
                                )
                                temp_posts_buffer_for_db_add.append(new_post_db_obj)
                                newly_added_post_objects_in_session.append(new_post_db_obj)
                                total_collected_for_channel_this_run += 1
                            
                            if temp_posts_buffer_for_db_add:
                                db_session.add_all(temp_posts_buffer_for_db_add)
                                print(f"    Добавлено в сессию {total_collected_for_channel_this_run} новых постов для канала '{current_channel_db_obj.title}'.")
                            
                            if latest_post_id_seen_this_run > (current_channel_db_obj.last_processed_post_id or 0):
                                current_channel_db_obj.last_processed_post_id = latest_post_id_seen_this_run
                                db_session.add(current_channel_db_obj) # SQLAlchemy отследит изменения
                                print(f"    Обновлен last_processed_post_id для канала '{current_channel_db_obj.title}' на {latest_post_id_seen_this_run}.")
                            elif total_collected_for_channel_this_run == 0:
                                print(f"    Новых постов для канала '{current_channel_db_obj.title}' не найдено (проверено ID > {initial_min_id_for_channel}).")
                        
                        if newly_added_post_objects_in_session:
                            print(f"  Начинаем сбор комментариев для {len(newly_added_post_objects_in_session)} новых постов...")
                            await db_session.flush() # Важно! Чтобы у new_post_db_obj появился автоинкрементный Post.id из БД

                            for new_post_db_obj_iter in newly_added_post_objects_in_session: # Используем новое имя переменной
                                print(f"    Сбор комментариев для поста Telegram ID {new_post_db_obj_iter.telegram_post_id} (наш внутренний ID: {new_post_db_obj_iter.id})")
                                comments_for_this_post_collected_count = 0
                                COMMENT_FETCH_LIMIT = settings.COMMENT_FETCH_LIMIT # Берем из config.py

                                try:
                                    async for comment_msg_tg in tg_client.iter_messages(
                                        entity=channel_entity,
                                        limit=COMMENT_FETCH_LIMIT,
                                        reply_to=new_post_db_obj_iter.telegram_post_id
                                    ):
                                        comment_msg_tg: Message
                                        if not comment_msg_tg.text: continue

                                        stmt_comment_check = select(Comment.id).where(Comment.telegram_comment_id == comment_msg_tg.id, Comment.post_id == new_post_db_obj_iter.id)
                                        result_comment_check = await db_session.execute(stmt_comment_check)
                                        if result_comment_check.scalar_one_or_none() is not None:
                                            continue
                                        
                                        user_tg_id, user_username_val, user_fullname_val = None, None, None
                                        if comment_msg_tg.sender_id:
                                            try:
                                                sender_entity = await tg_client.get_entity(comment_msg_tg.sender_id)
                                                if isinstance(sender_entity, User):
                                                    user_tg_id = sender_entity.id
                                                    user_username_val = sender_entity.username
                                                    user_fullname_val = f"{sender_entity.first_name or ''} {sender_entity.last_name or ''}".strip()
                                            except Exception as e_sender:
                                                print(f"      Не удалось получить инфо об отправителе коммента {comment_msg_tg.id} (sender_id: {comment_msg_tg.sender_id}): {type(e_sender).__name__} {e_sender}")

                                        new_comment_db_obj = Comment(
                                            telegram_comment_id=comment_msg_tg.id,
                                            post_id=new_post_db_obj_iter.id,
                                            telegram_user_id=user_tg_id,
                                            user_username=user_username_val,
                                            user_fullname=user_fullname_val,
                                            text_content=comment_msg_tg.text,
                                            commented_at=comment_msg_tg.date.replace(tzinfo=timezone.utc) if comment_msg_tg.date else datetime.now(timezone.utc)
                                        )
                                        db_session.add(new_comment_db_obj)
                                        comments_for_this_post_collected_count += 1
                                    
                                    if comments_for_this_post_collected_count > 0:
                                        new_post_db_obj_iter.comments_count = (new_post_db_obj_iter.comments_count or 0) + comments_for_this_post_collected_count
                                        db_session.add(new_post_db_obj_iter) # SQLAlchemy отследит
                                        print(f"      Добавлено/обновлено {comments_for_this_post_collected_count} комментариев для поста ID {new_post_db_obj_iter.telegram_post_id}")

                                except FloodWaitError as fwe_comment:
                                    print(f"    !!! FloodWaitError при сборе комментариев для поста {new_post_db_obj_iter.telegram_post_id}: ждем {fwe_comment.seconds} секунд.")
                                    await asyncio.sleep(fwe_comment.seconds + 5) # +5 для запаса
                                except Exception as e_comment_block:
                                    print(f"    Ошибка при сборе комментариев для поста {new_post_db_obj_iter.telegram_post_id}: {type(e_comment_block).__name__} {e_comment_block}")
                                    traceback.print_exc(limit=2)
                        else:
                            print(f"  Нет новых постов для сбора комментариев для канала '{current_channel_db_obj.title if current_channel_db_obj else channel_identifier}'.")

                    except FloodWaitError as fwe_channel:
                        print(f"  !!! FloodWaitError для канала {channel_identifier}: ждем {fwe_channel.seconds} секунд. Попробуем позже.")
                        await asyncio.sleep(fwe_channel.seconds + 5)
                        # Можно добавить self.retry(exc=fwe_channel) если хотим, чтобы Celery перепопытал таск
                    except ValueError as ve_channel: # Например, если канал не найден по идентификатору
                        print(f"  Ошибка (ValueError) при обработке канала {channel_identifier}: {ve_channel}. Пропускаем канал.")
                    except Exception as e_channel_processing:
                        print(f"  Неожиданная ошибка при обработке канала {channel_identifier}: {type(e_channel_processing).__name__} {e_channel_processing}")
                        traceback.print_exc(limit=3)
                
                await db_session.commit()
                print("\nВсе изменения (каналы, посты, комментарии) сохранены в БД.")
            return "Сбор данных (с постами и комментариями) завершен."

        except ConnectionRefusedError as e_auth: # Поймаем нашу ошибку авторизации
            # Не нужно делать tg_client.disconnect() или engine.dispose(), т.к. они могли не создаться
            # или ошибка произошла до их успешного создания/подключения.
            # Пробрасываем дальше, чтобы Celery пометил как FAILED без retry (если это не retryable ошибка)
            raise e_auth from e_auth # Сохраняем контекст ошибки
        except Exception as e_async_logic:
            print(f"!!! КРИТИЧЕСКАЯ ОШИБКА внутри _async_main_logic: {type(e_async_logic).__name__} {e_async_logic}")
            traceback.print_exc()
            raise # Пробрасываем, чтобы внешний try-except это поймал и Celery пометил как FAILED
        finally:
            if tg_client and tg_client.is_connected():
                print("Отключение Telegram клиента из _async_main_logic (finally)...")
                await tg_client.disconnect()
            if local_async_engine:
                print("Закрытие пула соединений БД (local_async_engine) из _async_main_logic (finally)...")
                await local_async_engine.dispose()
    
    # Запуск асинхронной логики из синхронного Celery таска
    try:
        # asyncio.run() автоматически управляет циклом событий.
        result_message = asyncio.run(_async_main_logic())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except ConnectionRefusedError as e_auth_final: # Ловим ошибку авторизации, проброшенную из _async_main_logic
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! ОШИБКА АВТОРИЗАЦИИ в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {e_auth_final}. Таск не будет повторен."
        print(final_error_message)
        # Не пробрасываем, чтобы Celery не пытался повторить ошибку авторизации.
        # Вместо этого возвращаем сообщение об ошибке, и таск будет SUCCESS в Celery, но с ошибкой в результате.
        # Если хотим FAILED без retry, нужно было бы настроить `autoretry_for` или пробросить特定исключение.
        # Для простоты, сейчас просто возвращаем строку.
        # Чтобы Celery показал FAILED, нужно пробросить исключение.
        raise e_auth_final from e_auth_final # Чтобы Celery пометил как FAILED
    except Exception as e_task_level:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}"
        print(final_error_message)
        traceback.print_exc()
        # Пробрасываем для стандартной обработки ошибок Celery (таск будет FAILED)
        # Можно добавить retry логику здесь, если ошибка временная
        try:
            print(f"Попытка retry для таска {self.request.id} из-за {type(e_task_level).__name__}")
            raise self.retry(exc=e_task_level, countdown=int(self.default_retry_delay * (self.request.retries + 1)))
        except self.MaxRetriesExceededError:
            print(f"Достигнуто максимальное количество попыток для таска {self.request.id}. Ошибка: {e_task_level}")
            # Если хотим, чтобы таск завершился как FAILED после всех retry
            raise e_task_level from e_task_level # Пробрасываем оригинальную ошибку
        except Exception as e_retry_logic: # Ловим ошибки самой логики retry (например, если self не доступен)
             print(f"Ошибка в логике retry: {e_retry_logic}")
             raise e_task_level from e_task_level # Все равно пробрасываем оригинальную ошибку

# --- END OF FILE tasks.py (Corrected) ---