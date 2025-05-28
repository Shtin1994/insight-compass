# app/tasks.py

import asyncio
import os
import time
import traceback
from datetime import timezone, datetime, timedelta

import openai
from openai import OpenAIError 

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select 
from sqlalchemy import desc, func, update # Добавлен update

import telegram
from telegram.constants import ParseMode
from telegram import helpers 

from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import MsgIdInvalidError
from telethon.tl.types import Message, User 
from telethon import TelegramClient

from app.celery_app import celery_instance
from app.core.config import settings
from app.models.telegram_data import Channel, Post, Comment 

# --- Ваши существующие задачи ---
@celery_instance.task(name="add")
def add(x, y):
    print(f"Тестовый таск 'add': {x} + {y}")
    time.sleep(5)
    result = x + y
    print(f"Результат 'add': {result}")
    return result

@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str):
    print(f"Тестовый таск 'simple_debug_task' получил сообщение: {message}")
    time.sleep(3)
    return f"Сообщение '{message}' обработано в simple_debug_task"

@celery_instance.task(name="collect_telegram_data", bind=True, max_retries=3, default_retry_delay=60)
def collect_telegram_data_task(self):
    # ... (полный код вашей задачи collect_telegram_data_task, как вы его предоставили) ...
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (сбор постов и КОММЕНТАРИЕВ)...")

    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    target_channels_list = settings.TARGET_TELEGRAM_CHANNELS

    if not all([api_id_val, api_hash_val, phone_number_val]):
        error_msg = "Ошибка: Необходимые учетные данные Telegram не настроены (TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE_NUMBER_FOR_LOGIN)."
        print(error_msg)
        return error_msg 

    session_file_path_in_container = "/app/my_telegram_session"
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic_collector():
        tg_client = None
        local_async_engine = None
        
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

                        is_first_fetch_for_channel = False
                        if not current_channel_db_obj:
                            print(f"  Канал '{channel_entity.title}' (ID: {channel_entity.id}) не найден в БД. Добавляем...")
                            new_channel_db_obj_for_add = Channel(
                                id=channel_entity.id,
                                username=getattr(channel_entity, 'username', None),
                                title=channel_entity.title,
                                description=getattr(channel_entity, 'about', None),
                                is_active=True
                            )
                            db_session.add(new_channel_db_obj_for_add)
                            await db_session.flush() 
                            current_channel_db_obj = new_channel_db_obj_for_add
                            is_first_fetch_for_channel = True
                            print(f"  Канал '{new_channel_db_obj_for_add.title}' добавлен в сессию БД с ID: {new_channel_db_obj_for_add.id}")
                        else:
                            print(f"  Канал '{channel_entity.title}' уже существует в БД. ID: {current_channel_db_obj.id}")
                        
                        if current_channel_db_obj:
                            print(f"  Начинаем сбор постов для канала '{current_channel_db_obj.title}'...")
                            
                            iter_messages_params = {
                                "entity": channel_entity,
                                "limit": settings.POST_FETCH_LIMIT,
                            }

                            if current_channel_db_obj.last_processed_post_id and current_channel_db_obj.last_processed_post_id > 0:
                                iter_messages_params["min_id"] = current_channel_db_obj.last_processed_post_id
                                print(f"  Последующий сбор: используем min_id={current_channel_db_obj.last_processed_post_id}.")
                            elif is_first_fetch_for_channel and settings.INITIAL_POST_FETCH_START_DATETIME:
                                iter_messages_params["offset_date"] = settings.INITIAL_POST_FETCH_START_DATETIME
                                iter_messages_params["reverse"] = True 
                                print(f"  Первый сбор (с датой): начинаем с offset_date={settings.INITIAL_POST_FETCH_START_DATETIME}, reverse=True.")
                            else:
                                print(f"  Первый сбор (без даты): собираем последние {settings.POST_FETCH_LIMIT} постов.")

                            latest_post_id_seen_this_run = current_channel_db_obj.last_processed_post_id or 0
                            total_collected_for_channel_this_run = 0
                            temp_posts_buffer_for_db_add: list[Post] = []

                            async for message_tg in tg_client.iter_messages(**iter_messages_params):
                                message_tg: Message
                                if not (message_tg.text or message_tg.media): continue
                                
                                if message_tg.id > latest_post_id_seen_this_run:
                                    latest_post_id_seen_this_run = message_tg.id
                                elif not (is_first_fetch_for_channel and settings.INITIAL_POST_FETCH_START_DATETIME and iter_messages_params.get("reverse")):
                                    pass

                                stmt_post_check = select(Post.id).where(Post.telegram_post_id == message_tg.id, Post.channel_id == current_channel_db_obj.id)
                                result_post_check = await db_session.execute(stmt_post_check)
                                if result_post_check.scalar_one_or_none() is not None:
                                    continue 

                                print(f"    Найден новый пост ID {message_tg.id}. Текст: '{message_tg.text[:50].replace(chr(10),' ') if message_tg.text else '[Медиа]'}'...")
                                post_link = f"https://t.me/{current_channel_db_obj.username}/{message_tg.id}" if current_channel_db_obj.username else f"https://t.me/c/{current_channel_db_obj.id}/{message_tg.id}"
                                
                                new_post_db_obj = Post(
                                    telegram_post_id=message_tg.id,
                                    channel_id=current_channel_db_obj.id,
                                    link=post_link,
                                    text_content=message_tg.text,
                                    views_count=message_tg.views,
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
                                db_session.add(current_channel_db_obj)
                                print(f"    Обновлен last_processed_post_id для канала '{current_channel_db_obj.title}' на {latest_post_id_seen_this_run}.")
                            elif total_collected_for_channel_this_run == 0:
                                print(f"    Новых постов для канала '{current_channel_db_obj.title}' не найдено.")
                        
                        if newly_added_post_objects_in_session:
                            print(f"  Начинаем сбор комментариев для {len(newly_added_post_objects_in_session)} новых постов...")
                            await db_session.flush()

                            for new_post_db_obj_iter in newly_added_post_objects_in_session:
                                print(f"    Сбор комментариев для поста Telegram ID {new_post_db_obj_iter.telegram_post_id} (наш внутренний ID: {new_post_db_obj_iter.id})")
                                comments_for_this_post_collected_count = 0
                                COMMENT_FETCH_LIMIT = settings.COMMENT_FETCH_LIMIT

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
                                            except FloodWaitError as fwe_user: 
                                                print(f"      FloodWaitError при получении инфо об отправителе {comment_msg_tg.sender_id} для коммента {comment_msg_tg.id}: ждем {fwe_user.seconds} сек.")
                                                await asyncio.sleep(fwe_user.seconds + 5)
                                                try:
                                                    sender_entity = await tg_client.get_entity(comment_msg_tg.sender_id)
                                                    if isinstance(sender_entity, User):
                                                        user_tg_id = sender_entity.id
                                                        user_username_val = sender_entity.username
                                                        user_fullname_val = f"{sender_entity.first_name or ''} {sender_entity.last_name or ''}".strip()
                                                except Exception as e_sender_retry:
                                                     print(f"      Повторная попытка получить инфо об отправителе {comment_msg_tg.sender_id} не удалась: {type(e_sender_retry).__name__}")
                                            except Exception as e_sender:
                                                print(f"      Не удалось получить инфо об отправителе коммента {comment_msg_tg.id} (sender_id: {comment_msg_tg.sender_id}): {type(e_sender).__name__} {e_sender}")

                                        new_comment_db_obj = Comment(
                                            telegram_comment_id=comment_msg_tg.id,
                                            post_id=new_post_db_obj_iter.id,
                                            telegram_user_id=user_tg_id,
                                            user_username=user_username_val,
                                            user_fullname=user_fullname_val,
                                            text_content=comment_msg_tg.text, # Убедимся, что это поле из вашей модели Comment
                                            commented_at=comment_msg_tg.date.replace(tzinfo=timezone.utc) if comment_msg_tg.date else datetime.now(timezone.utc)
                                        )
                                        db_session.add(new_comment_db_obj)
                                        comments_for_this_post_collected_count += 1
                                    
                                    if comments_for_this_post_collected_count > 0:
                                        new_post_db_obj_iter.comments_count = (new_post_db_obj_iter.comments_count or 0) + comments_for_this_post_collected_count
                                        db_session.add(new_post_db_obj_iter)
                                        print(f"      Добавлено/обновлено {comments_for_this_post_collected_count} комментариев для поста ID {new_post_db_obj_iter.telegram_post_id}")
                                
                                except MsgIdInvalidError:
                                    print(f"    Не удалось найти комментарии для поста ID {new_post_db_obj_iter.telegram_post_id} (MsgIdInvalid). Возможно, их нет или они отключены.")
                                except FloodWaitError as fwe_comment:
                                    print(f"    !!! FloodWaitError при сборе комментариев для поста {new_post_db_obj_iter.telegram_post_id}: ждем {fwe_comment.seconds} секунд.")
                                    await asyncio.sleep(fwe_comment.seconds + 5)
                                except Exception as e_comment_block:
                                    print(f"    Ошибка при сборе комментариев для поста {new_post_db_obj_iter.telegram_post_id}: {type(e_comment_block).__name__} {e_comment_block}")
                                    traceback.print_exc(limit=2)
                        else:
                            print(f"  Нет новых постов для сбора комментариев для канала '{current_channel_db_obj.title if current_channel_db_obj else channel_identifier}'.")

                    except FloodWaitError as fwe_channel:
                        print(f"  !!! FloodWaitError для канала {channel_identifier}: ждем {fwe_channel.seconds} секунд. Попробуем позже.")
                        await asyncio.sleep(fwe_channel.seconds + 5)
                    except ValueError as ve_channel: 
                        print(f"  Ошибка (ValueError) при обработке канала {channel_identifier}: {ve_channel}. Пропускаем канал.")
                    except Exception as e_channel_processing:
                        print(f"  Неожиданная ошибка при обработке канала {channel_identifier}: {type(e_channel_processing).__name__} {e_channel_processing}")
                        traceback.print_exc(limit=3)
                
                await db_session.commit()
                print("\nВсе изменения (каналы, посты, комментарии) сохранены в БД.")
            return "Сбор данных (с постами и комментариями) завершен."

        except ConnectionRefusedError as e_auth:
            raise e_auth from e_auth 
        except Exception as e_async_logic:
            print(f"!!! КРИТИЧЕСКАЯ ОШИБКА внутри _async_main_logic_collector: {type(e_async_logic).__name__} {e_async_logic}")
            traceback.print_exc()
            raise 
        finally:
            if tg_client and tg_client.is_connected():
                print("Отключение Telegram клиента из _async_main_logic_collector (finally)...")
                await tg_client.disconnect()
            if local_async_engine:
                print("Закрытие пула соединений БД (local_async_engine) из _async_main_logic_collector (finally)...")
                await local_async_engine.dispose()
    
    try:
        result_message = asyncio.run(_async_main_logic_collector())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except ConnectionRefusedError as e_auth_final:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! ОШИБКА АВТОРИЗАЦИИ в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {e_auth_final}. Таск не будет повторен."
        print(final_error_message)
        raise e_auth_final from e_auth_final 
    except Exception as e_task_level:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}"
        print(final_error_message)
        traceback.print_exc()
        try:
            print(f"Попытка retry для таска {self.request.id} из-за {type(e_task_level).__name__}")
            raise self.retry(exc=e_task_level, countdown=int(self.default_retry_delay * (self.request.retries + 1)))
        except self.MaxRetriesExceededError:
            print(f"Достигнуто максимальное количество попыток для таска {self.request.id}. Ошибка: {e_task_level}")
            raise e_task_level from e_task_level
        except Exception as e_retry_logic:
             print(f"Ошибка в логике retry: {e_retry_logic}")
             raise e_task_level from e_task_level

@celery_instance.task(name="summarize_top_posts", bind=True, max_retries=2, default_retry_delay=300)
def summarize_top_posts_task(self, hours_ago=48, top_n=3):
    # ... (полный код вашей задачи summarize_top_posts_task, как вы его предоставили) ...
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (AI Суммаризация топ-{top_n} постов за {hours_ago}ч)...")

    if not settings.OPENAI_API_KEY:
        error_msg = "Ошибка: OPENAI_API_KEY не настроен в .env файле."
        print(error_msg)
        return error_msg 

    try:
        openai_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    except Exception as e_openai_init:
        error_msg = f"Ошибка инициализации OpenAI клиента: {e_openai_init}"
        print(error_msg)
        try:
            raise self.retry(exc=e_openai_init)
        except self.MaxRetriesExceededError:
            return error_msg
        except Exception as e_retry_logic_openai:
            print(f"Ошибка в логике retry для OpenAI init: {e_retry_logic_openai}")
            return error_msg

    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic_summarizer():
        local_async_engine = None
        processed_posts_count = 0
        
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(
                bind=local_async_engine, class_=AsyncSession, expire_on_commit=False
            )

            async with LocalAsyncSessionFactory() as db_session:
                time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
                
                stmt_posts_to_summarize = (
                    select(Post)
                    .where(Post.posted_at >= time_threshold)
                    .where(Post.summary_text == None) 
                    .order_by(desc(Post.comments_count)) 
                    .limit(top_n)
                )
                
                result_posts = await db_session.execute(stmt_posts_to_summarize)
                posts_to_process = result_posts.scalars().all()

                if not posts_to_process:
                    print(f"  Не найдено постов для суммаризации (за последние {hours_ago}ч, топ-{top_n}, без summary_text).")
                    return "Нет постов для суммаризации."

                print(f"  Найдено {len(posts_to_process)} постов для суммаризации.")

                for post_obj in posts_to_process:
                    post_obj: Post
                    if not post_obj.text_content or len(post_obj.text_content.strip()) < 50 : 
                        print(f"    Пост ID {post_obj.id} (TG ID: {post_obj.telegram_post_id}) слишком короткий или без текста, пропускаем суммаризацию.")
                        continue

                    print(f"    Суммаризация поста ID {post_obj.id} (TG ID: {post_obj.telegram_post_id}), комментариев: {post_obj.comments_count}...")
                    
                    try:
                        summary_prompt = f"""
                        Контекст: Ты - AI-аналитик, помогающий маркетологам быстро понять суть обсуждений в Telegram-каналах.
                        Задача: Прочитай следующий пост из Telegram-канала и напиши краткое резюме (1-3 предложения на русском языке), отражающее его основную мысль или тему обсуждения. Резюме должно быть нейтральным и информативным.

                        Текст поста:
                        ---
                        {post_obj.text_content[:4000]} 
                        ---

                        Краткое резюме (1-3 предложения):
                        """ 
                        
                        completion = await asyncio.to_thread( 
                            openai_client.chat.completions.create,
                            model="gpt-3.5-turbo", 
                            messages=[
                                {"role": "system", "content": "Ты полезный AI-ассистент, который генерирует краткие резюме текстов на русском языке."},
                                {"role": "user", "content": summary_prompt}
                            ],
                            temperature=0.3, 
                            max_tokens=150   
                        )
                        
                        summary = completion.choices[0].message.content.strip()
                        
                        if summary:
                            post_obj.summary_text = summary
                            post_obj.updated_at = datetime.now(timezone.utc) 
                            db_session.add(post_obj)
                            processed_posts_count += 1
                            print(f"      Резюме для поста ID {post_obj.id} получено и сохранено: '{summary[:100]}...'")
                        else:
                            print(f"      OpenAI вернул пустое резюме для поста ID {post_obj.id}.")

                    except OpenAIError as e_openai:
                        print(f"    !!! Ошибка OpenAI API при суммаризации поста ID {post_obj.id}: {type(e_openai).__name__} - {e_openai}")
                        continue 
                    except Exception as e_summary:
                        print(f"    !!! Неожиданная ошибка при суммаризации поста ID {post_obj.id}: {type(e_summary).__name__} - {e_summary}")
                        traceback.print_exc(limit=2)
                        continue
                
                if processed_posts_count > 0:
                    await db_session.commit()
                    print(f"  Успешно суммаризировано и сохранено {processed_posts_count} постов.")
                else:
                    print(f"  Не было суммаризировано ни одного поста в этом запуске (возможно, из-за ошибок или пустых ответов LLM).")

            return f"Суммаризация завершена. Обработано: {processed_posts_count} постов."

        except Exception as e_async_summarizer:
            print(f"!!! КРИТИЧЕСКАЯ ОШИБКА внутри _async_main_logic_summarizer: {type(e_async_summarizer).__name__} {e_async_summarizer}")
            traceback.print_exc()
            raise
        finally:
            if local_async_engine:
                print("Закрытие пула соединений БД (local_async_engine) из _async_main_logic_summarizer (finally)...")
                await local_async_engine.dispose()

    try:
        result_message = asyncio.run(_async_main_logic_summarizer())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_level_summarizer:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {type(e_task_level_summarizer).__name__} {e_task_level_summarizer}"
        print(final_error_message)
        traceback.print_exc()
        try:
            print(f"Попытка retry для таска {self.request.id} (summarizer) из-за {type(e_task_level_summarizer).__name__}")
            raise self.retry(exc=e_task_level_summarizer, countdown=int(self.default_retry_delay * (self.request.retries + 1)))
        except self.MaxRetriesExceededError:
            print(f"Достигнуто максимальное количество попыток для таска {self.request.id} (summarizer). Ошибка: {e_task_level_summarizer}")
            raise e_task_level_summarizer from e_task_level_summarizer
        except Exception as e_retry_logic_summarizer:
             print(f"Ошибка в логике retry (summarizer): {e_retry_logic_summarizer}")
             raise e_task_level_summarizer from e_task_level_summarizer

@celery_instance.task(name="send_daily_digest", bind=True, max_retries=3, default_retry_delay=180)
def send_daily_digest_task(self, hours_ago_posts=24, top_n_summarized=3):
    # ... (полный код вашей задачи send_daily_digest_task, как вы его предоставили) ...
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Отправка ежедневного дайджеста)...")

    if not settings.TELEGRAM_BOT_TOKEN:
        error_msg = "Ошибка: TELEGRAM_BOT_TOKEN не настроен в .env файле для отправки дайджеста."
        print(error_msg)
        return error_msg
    if not settings.TELEGRAM_TARGET_CHAT_ID:
        error_msg = "Ошибка: TELEGRAM_TARGET_CHAT_ID не настроен в .env файле для отправки дайджеста."
        print(error_msg)
        return error_msg

    async def _async_send_digest_logic():
        bot = telegram.Bot(token=settings.TELEGRAM_BOT_TOKEN)
        ASYNC_DB_URL_FOR_TASK_DIGEST = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
        
        local_async_engine_digest = None
        message_parts = []
        result_status_internal = "Не удалось отправить дайджест." 

        try:
            local_async_engine_digest = create_async_engine(ASYNC_DB_URL_FOR_TASK_DIGEST, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactoryDigest = sessionmaker(
                bind=local_async_engine_digest, class_=AsyncSession, expire_on_commit=False
            )

            async with LocalAsyncSessionFactoryDigest() as db_session:
                time_threshold_posts = datetime.now(timezone.utc) - timedelta(hours=hours_ago_posts)
                
                stmt_new_posts_count = select(func.count(Post.id)).where(Post.posted_at >= time_threshold_posts)
                result_new_posts_count = await db_session.execute(stmt_new_posts_count)
                new_posts_count = result_new_posts_count.scalar_one_or_none() or 0

                header_part = helpers.escape_markdown(f" digest for *Insight-Compass* за последние {hours_ago_posts} часа:\n", version=2)
                message_parts.append(header_part)

                new_posts_summary_part = helpers.escape_markdown(f"📰 Всего новых постов: *{new_posts_count}*\n", version=2)
                message_parts.append(new_posts_summary_part)
                
                stmt_top_posts = (
                    select(Post.link, Post.comments_count, Post.summary_text, Channel.title.label("channel_title"))
                    .join(Channel, Post.channel_id == Channel.id)
                    .where(Post.posted_at >= time_threshold_posts)
                    .where(Post.comments_count > 0)         
                    .where(Post.summary_text != None)       
                    .order_by(desc(Post.comments_count))
                    .limit(top_n_summarized)
                )
                result_top_posts = await db_session.execute(stmt_top_posts)
                top_posts_data = result_top_posts.all()

                if top_posts_data:
                    top_posts_header_part = helpers.escape_markdown(f"\n🔥 Топ-{len(top_posts_data)} обсуждаемых постов с AI-резюме:\n", version=2)
                    message_parts.append(top_posts_header_part)
                    
                    for i, post_data in enumerate(top_posts_data):
                        link_url = post_data.link 
                        link_text = "Пост" 
                        comments = post_data.comments_count 
                        summary_text_original = post_data.summary_text 
                        summary_escaped = helpers.escape_markdown(summary_text_original, version=2)
                        channel_title_original = post_data.channel_title or "Неизвестный канал"
                        channel_title_escaped = helpers.escape_markdown(channel_title_original, version=2)
                        item_number_str = helpers.escape_markdown(str(i+1), version=2) + "\\."
                        post_digest_part_str = (
                            f"\n*{item_number_str}* {channel_title_escaped} [{link_text}]({link_url})\n"
                            f"   💬 Комментариев: {comments}\n"
                            f"   📝 Резюме: _{summary_escaped}_\n"
                        )
                        message_parts.append(post_digest_part_str)
                else:
                    no_top_posts_part = helpers.escape_markdown("\n🔥 Нет активно обсуждаемых постов с готовыми резюме за указанный период.\nПопробуйте сначала запустить AI-суммаризацию или дождитесь следующего цикла.\n", version=2)
                    message_parts.append(no_top_posts_part)

            digest_message_final = "".join(message_parts)
            
            await bot.send_message(
                chat_id=settings.TELEGRAM_TARGET_CHAT_ID,
                text=digest_message_final,
                parse_mode=ParseMode.MARKDOWN_V2,
                disable_web_page_preview=True
            )
            result_status_internal = f"Дайджест успешно отправлен. Постов: {new_posts_count}, Топ: {len(top_posts_data)}."

        except telegram.error.TelegramError as e_tg_bot_internal:
            error_msg_internal = f"!!! Ошибка Telegram Bot API при отправке дайджеста: {type(e_tg_bot_internal).__name__} - {e_tg_bot_internal}"
            print(error_msg_internal)
            result_status_internal = error_msg_internal
            raise e_tg_bot_internal 
            
        except Exception as e_digest_internal:
            error_msg_internal = f"!!! Неожиданная ошибка при формировании/отправке дайджеста: {type(e_digest_internal).__name__} - {e_digest_internal}"
            print(error_msg_internal)
            traceback.print_exc()
            result_status_internal = error_msg_internal
            raise e_digest_internal
        finally:
            if local_async_engine_digest:
                await local_async_engine_digest.dispose()
        
        return result_status_internal

    try:
        result_message = asyncio.run(_async_send_digest_logic())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_level_digest:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {type(e_task_level_digest).__name__} {e_task_level_digest}"
        print(final_error_message)
        traceback.print_exc()
        try:
            print(f"Попытка retry для таска {self.request.id} (digest) из-за {type(e_task_level_digest).__name__}")
            raise self.retry(exc=e_task_level_digest, countdown=int(self.default_retry_delay * (self.request.retries + 1)))
        except self.MaxRetriesExceededError:
            print(f"Достигнуто максимальное количество попыток для таска {self.request.id} (digest). Ошибка: {e_task_level_digest}")
            raise e_task_level_digest from e_task_level_digest
        except Exception as e_retry_logic_digest:
             print(f"Ошибка в логике retry (digest): {e_retry_logic_digest}")
             raise e_task_level_digest from e_task_level_digest

# --- НОВАЯ ЗАДАЧА: Анализ тональности постов ---
@celery_instance.task(name="analyze_posts_sentiment", bind=True, max_retries=2, default_retry_delay=300)
def analyze_posts_sentiment_task(self, limit_posts_to_analyze=10):
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Анализ тональности постов, лимит: {limit_posts_to_analyze})...")

    if not settings.OPENAI_API_KEY: 
        error_msg = "Ошибка: OPENAI_API_KEY не настроен для анализа тональности."
        print(error_msg)
        return error_msg 

    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic_sentiment_analyzer():
        local_async_engine = None
        analyzed_posts_count = 0
        
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(
                bind=local_async_engine, class_=AsyncSession, expire_on_commit=False
            )

            async with LocalAsyncSessionFactory() as db_session:
                stmt_posts_to_analyze = (
                    select(Post)
                    .where(Post.text_content != None) # Используем text_content для проверки наличия текста поста
                    .where(Post.post_sentiment_label == None) 
                    .order_by(Post.posted_at.asc()) 
                    .limit(limit_posts_to_analyze)
                )
                
                result_posts = await db_session.execute(stmt_posts_to_analyze)
                posts_to_process = result_posts.scalars().all()

                if not posts_to_process:
                    print(f"  Не найдено постов для анализа тональности (с текстом и без sentiment_label).")
                    return "Нет постов для анализа тональности."

                print(f"  Найдено {len(posts_to_process)} постов для анализа тональности.")

                for post_obj in posts_to_process:
                    post_obj: Post
                    print(f"    Обработка поста ID {post_obj.id} (TG ID: {post_obj.telegram_post_id}) для анализа тональности...")
                    
                    mock_sentiment_label = "neutral" 
                    mock_sentiment_score = 0.0       
                    await asyncio.sleep(0.1) 
                    
                    post_obj.post_sentiment_label = mock_sentiment_label
                    post_obj.post_sentiment_score = mock_sentiment_score
                    post_obj.updated_at = datetime.now(timezone.utc)
                    db_session.add(post_obj) 

                    analyzed_posts_count += 1
                    print(f"      Тональность для поста ID {post_obj.id} установлена (mock): {mock_sentiment_label} ({mock_sentiment_score})")
                
                if analyzed_posts_count > 0:
                    await db_session.commit()
                    print(f"  Успешно проанализирована тональность (mock) и сохранено {analyzed_posts_count} постов.")
                else:
                    print(f"  Не было проанализировано ни одного поста в этом запуске.")

            return f"Анализ тональности (mock) завершен. Обработано: {analyzed_posts_count} постов."

        except Exception as e_async_analyzer:
            print(f"!!! КРИТИЧЕСКАЯ ОШИБКА внутри _async_main_logic_sentiment_analyzer: {type(e_async_analyzer).__name__} {e_async_analyzer}")
            traceback.print_exc()
            raise
        finally:
            if local_async_engine:
                await local_async_engine.dispose()

    try:
        result_message = asyncio.run(_async_main_logic_sentiment_analyzer())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_level_analyzer:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {type(e_task_level_analyzer).__name__} {e_task_level_analyzer}"
        print(final_error_message)
        traceback.print_exc()
        try:
            raise self.retry(exc=e_task_level_analyzer)
        except self.MaxRetriesExceededError:
            raise e_task_level_analyzer from e_task_level_analyzer
        except Exception as e_retry_logic_analyzer:
             raise e_task_level_analyzer from e_retry_logic_analyzer
# --- Конец задачи анализа тональности ---