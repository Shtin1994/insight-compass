# app/tasks.py

import asyncio
import os
import time
import traceback
import json
from datetime import timezone, datetime, timedelta

import openai
from openai import OpenAIError

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy import desc, func, update

import telegram
from telegram.constants import ParseMode
from telegram import helpers

from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.errors.rpcerrorlist import MsgIdInvalidError
# ОБНОВЛЕННЫЙ ИМПОРТ для более точной проверки типов
from telethon.tl.types import Message, User as TelethonUserType, Channel as TelethonChannelType, Chat as TelethonChatType
from telethon import TelegramClient

from app.celery_app import celery_instance
from app.core.config import settings
from app.models.telegram_data import Channel, Post, Comment

# --- Существующие задачи (add, simple_debug_task - без изменений) ---
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

# --- ОБНОВЛЕННАЯ ЗАДАЧА СБОРА ДАННЫХ ---
@celery_instance.task(name="collect_telegram_data", bind=True, max_retries=3, default_retry_delay=60 * 5)
def collect_telegram_data_task(self):
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (сбор постов и КОММЕНТАРИЕВ из БД)...")

    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN

    if not all([api_id_val, api_hash_val, phone_number_val]):
        error_msg = "Ошибка: Необходимые учетные данные Telegram не настроены."
        print(error_msg)
        return error_msg

    session_file_path_in_container = "/app/my_telegram_session"
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic_collector():
        tg_client = None
        local_async_engine = None
        total_channels_processed = 0
        total_posts_collected = 0
        total_comments_collected = 0

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
                error_auth_msg = (f"ОШИБКА: Пользователь не авторизован для сессии {session_file_path_in_container}. "
                                  f"Запустите test_telegram_connection.py или процесс логина.")
                print(error_auth_msg)
                raise ConnectionRefusedError(error_auth_msg)

            me = await tg_client.get_me()
            print(f"Celery таск успешно подключен к Telegram как: {me.first_name} (@{me.username or ''}, ID: {me.id})")

            async with LocalAsyncSessionFactory() as db_session:
                stmt_active_channels = select(Channel).where(Channel.is_active == True)
                result_active_channels = await db_session.execute(stmt_active_channels)
                active_channels_from_db: List[Channel] = result_active_channels.scalars().all()

                if not active_channels_from_db:
                    print("В базе данных нет активных каналов для отслеживания. Завершение задачи.")
                    return "Нет активных каналов в БД."

                print(f"Найдено {len(active_channels_from_db)} активных каналов в БД для обработки.")

                for channel_db_obj in active_channels_from_db:
                    channel_db_obj: Channel
                    print(f"\nОбработка канала из БД: '{channel_db_obj.title}' (ID: {channel_db_obj.id}, Username: @{channel_db_obj.username or 'N/A'})")
                    total_channels_processed += 1
                    newly_added_post_objects_in_session: list[Post] = []

                    try:
                        channel_entity_tg = await tg_client.get_entity(channel_db_obj.id)

                        # --- ИСПРАВЛЕННАЯ ЛОГИКА ПРОВЕРКИ ТИПА СУЩНОСТИ ЗДЕСЬ ---
                        if not isinstance(channel_entity_tg, TelethonChannelType):
                            error_msg_type = f"  Сущность для ID {channel_db_obj.id} ('{channel_db_obj.title}') в Telegram оказалась '{type(channel_entity_tg).__name__}', а не Channel/Supergroup. Деактивируем."
                            if isinstance(channel_entity_tg, TelethonChatType):
                                error_msg_type = f"  Сущность для ID {channel_db_obj.id} ('{channel_db_obj.title}') в Telegram является базовой группой (Chat), не поддерживается. Деактивируем."
                            elif isinstance(channel_entity_tg, TelethonUserType):
                                error_msg_type = f"  Сущность для ID {channel_db_obj.id} ('{channel_db_obj.title}') в Telegram является пользователем (User), не канал. Деактивируем."
                            print(error_msg_type)
                            channel_db_obj.is_active = False
                            db_session.add(channel_db_obj)
                            continue

                        is_broadcast = getattr(channel_entity_tg, 'broadcast', False)
                        is_megagroup = getattr(channel_entity_tg, 'megagroup', False)

                        if not (is_broadcast or is_megagroup):
                            print(f"  Сущность для ID {channel_db_obj.id} ('{channel_db_obj.title}') является Channel-like, но не broadcast-каналом или супергруппой. Деактивируем.")
                            channel_db_obj.is_active = False
                            db_session.add(channel_db_obj)
                            continue
                        # --- КОНЕЦ ИСПРАВЛЕННОЙ ЛОГИКИ ---

                        if channel_entity_tg.title != channel_db_obj.title or \
                           getattr(channel_entity_tg, 'username', None) != channel_db_obj.username or \
                           getattr(channel_entity_tg, 'about', None) != channel_db_obj.description:
                            
                            print(f"  Обнаружены изменения в метаданных канала '{channel_db_obj.title}'. Обновляем в БД...")
                            channel_db_obj.title = channel_entity_tg.title
                            channel_db_obj.username = getattr(channel_entity_tg, 'username', None)
                            channel_db_obj.description = getattr(channel_entity_tg, 'about', None)
                            db_session.add(channel_db_obj)

                        print(f"  Начинаем сбор постов для канала '{channel_db_obj.title}'...")
                        iter_messages_params = {
                            "entity": channel_entity_tg,
                            "limit": settings.POST_FETCH_LIMIT,
                        }

                        if channel_db_obj.last_processed_post_id and channel_db_obj.last_processed_post_id > 0:
                            iter_messages_params["min_id"] = channel_db_obj.last_processed_post_id
                            print(f"  Последующий сбор: используем min_id={channel_db_obj.last_processed_post_id}.")
                        elif settings.INITIAL_POST_FETCH_START_DATETIME:
                            iter_messages_params["offset_date"] = settings.INITIAL_POST_FETCH_START_DATETIME
                            iter_messages_params["reverse"] = True
                            print(f"  Первый сбор (с датой): начинаем с offset_date={settings.INITIAL_POST_FETCH_START_DATETIME}, reverse=True.")
                        else:
                            print(f"  Первый сбор (без даты): собираем последние {settings.POST_FETCH_LIMIT} постов.")

                        latest_post_id_seen_this_run = channel_db_obj.last_processed_post_id or 0
                        collected_for_this_channel_this_run = 0
                        temp_posts_buffer_for_db_add: list[Post] = []

                        async for message_tg in tg_client.iter_messages(**iter_messages_params):
                            message_tg: Message
                            if not (message_tg.text or message_tg.media): continue

                            if message_tg.id > latest_post_id_seen_this_run:
                                latest_post_id_seen_this_run = message_tg.id
                            
                            stmt_post_check = select(Post.id).where(Post.telegram_post_id == message_tg.id, Post.channel_id == channel_db_obj.id)
                            result_post_check = await db_session.execute(stmt_post_check)
                            if result_post_check.scalar_one_or_none() is not None:
                                continue
                            
                            # Формирование ссылки:
                            link_username_part = channel_db_obj.username
                            if not link_username_part and not getattr(channel_entity_tg, 'megagroup', False) : # Для каналов без username (не супергрупп)
                                link_username_part = f"c/{channel_db_obj.id}"
                            
                            if not link_username_part and getattr(channel_entity_tg, 'megagroup', False):
                                # Для супергрупп без username, ссылка может быть сложнее или недоступна таким образом
                                # Можно попробовать message_tg.export_link(), если версия Telethon позволяет и права есть
                                # или просто оставить ее пустой/заглушкой
                                print(f"    Предупреждение: Не удалось сформировать простую ссылку для поста ID {message_tg.id} из мегагруппы без username (ID: {channel_db_obj.id}).")
                                post_link = f"https://t.me/c/{channel_db_obj.id}/{message_tg.id}" # Попытка стандартной c/ID/postID
                            elif link_username_part:
                                post_link = f"https://t.me/{link_username_part}/{message_tg.id}"
                            else: # Непредвиденный случай
                                post_link = "#" 


                            new_post_db_obj = Post(
                                telegram_post_id=message_tg.id,
                                channel_id=channel_db_obj.id,
                                link=post_link,
                                text_content=message_tg.text,
                                views_count=message_tg.views,
                                posted_at=message_tg.date.replace(tzinfo=timezone.utc) if message_tg.date else datetime.now(timezone.utc)
                            )
                            temp_posts_buffer_for_db_add.append(new_post_db_obj)
                            newly_added_post_objects_in_session.append(new_post_db_obj)
                            collected_for_this_channel_this_run += 1
                            total_posts_collected += 1
                            print(f"    Найден новый пост ID {message_tg.id}. Добавлен в буфер. Текст: '{message_tg.text[:30].replace(chr(10),' ') if message_tg.text else '[Медиа]'}'...")


                        if temp_posts_buffer_for_db_add:
                            db_session.add_all(temp_posts_buffer_for_db_add)
                            print(f"    Добавлено в сессию {collected_for_this_channel_this_run} новых постов для канала '{channel_db_obj.title}'.")

                        if latest_post_id_seen_this_run > (channel_db_obj.last_processed_post_id or 0):
                            channel_db_obj.last_processed_post_id = latest_post_id_seen_this_run
                            db_session.add(channel_db_obj)
                            print(f"    Обновлен last_processed_post_id для канала '{channel_db_obj.title}' на {latest_post_id_seen_this_run}.")
                        elif collected_for_this_channel_this_run == 0:
                            print(f"    Новых постов для канала '{channel_db_obj.title}' не найдено.")

                        if newly_added_post_objects_in_session:
                            print(f"  Начинаем сбор комментариев для {len(newly_added_post_objects_in_session)} новых постов канала '{channel_db_obj.title}'...")
                            await db_session.flush()

                            for new_post_db_obj_iter in newly_added_post_objects_in_session:
                                print(f"    Сбор комментариев для поста Telegram ID {new_post_db_obj_iter.telegram_post_id} (DB ID: {new_post_db_obj_iter.id})")
                                comments_for_this_post_collected_count = 0
                                COMMENT_FETCH_LIMIT = settings.COMMENT_FETCH_LIMIT

                                try:
                                    async for comment_msg_tg in tg_client.iter_messages(
                                        entity=channel_entity_tg,
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
                                                if isinstance(sender_entity, TelethonUserType):
                                                    user_tg_id = sender_entity.id
                                                    user_username_val = sender_entity.username
                                                    user_fullname_val = f"{sender_entity.first_name or ''} {sender_entity.last_name or ''}".strip()
                                            except FloodWaitError as fwe_user:
                                                print(f"      FloodWaitError при получении инфо об отправителе {comment_msg_tg.sender_id}: ждем {fwe_user.seconds} сек.")
                                                await asyncio.sleep(fwe_user.seconds + 2)
                                            except Exception as e_sender:
                                                print(f"      Не удалось получить инфо об отправителе коммента {comment_msg_tg.id} (sender_id: {comment_msg_tg.sender_id}): {type(e_sender).__name__}")

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
                                        total_comments_collected += 1

                                    if comments_for_this_post_collected_count > 0:
                                        stmt_update_post_comments = (
                                            update(Post)
                                            .where(Post.id == new_post_db_obj_iter.id)
                                            .values(comments_count=Post.comments_count + comments_for_this_post_collected_count) # Обновляем существующее значение
                                        )
                                        await db_session.execute(stmt_update_post_comments)
                                        print(f"      Добавлено {comments_for_this_post_collected_count} комментариев для поста ID {new_post_db_obj_iter.telegram_post_id}. Обновлен счетчик поста.")

                                except MsgIdInvalidError:
                                    print(f"    Не удалось найти комментарии для поста ID {new_post_db_obj_iter.telegram_post_id} (MsgIdInvalid).")
                                except FloodWaitError as fwe_comment:
                                    print(f"    !!! FloodWaitError при сборе комментариев для поста {new_post_db_obj_iter.telegram_post_id}: ждем {fwe_comment.seconds} секунд.")
                                    await asyncio.sleep(fwe_comment.seconds + 5)
                                except Exception as e_comment_block:
                                    print(f"    Ошибка при сборе комментариев для поста {new_post_db_obj_iter.telegram_post_id}: {type(e_comment_block).__name__} {e_comment_block}")
                                    traceback.print_exc(limit=1)
                        else:
                            print(f"  Нет новых постов для сбора комментариев для канала '{channel_db_obj.title}'.")

                    except (ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError) as e_channel_access:
                        print(f"  Канал ID {channel_db_obj.id} ('{channel_db_obj.title}') недоступен: {e_channel_access}. Деактивируем.")
                        channel_db_obj.is_active = False
                        db_session.add(channel_db_obj)
                    except FloodWaitError as fwe_channel:
                        print(f"  !!! FloodWaitError для канала {channel_db_obj.title} (ID: {channel_db_obj.id}): ждем {fwe_channel.seconds} секунд.")
                        await asyncio.sleep(fwe_channel.seconds + 5)
                    except Exception as e_channel_processing:
                        print(f"  Неожиданная ошибка при обработке канала {channel_db_obj.title} (ID: {channel_db_obj.id}): {type(e_channel_processing).__name__} {e_channel_processing}")
                        traceback.print_exc(limit=2)

                await db_session.commit()
                print("\nВсе изменения (каналы, посты, комментарии) сохранены в БД.")
            return f"Сбор данных завершен. Обработано каналов: {total_channels_processed}. Собрано постов: {total_posts_collected}. Собрано комментариев: {total_comments_collected}."

        except ConnectionRefusedError as e_auth:
            print(f"!!! ОШИБКА АВТОРИЗАЦИИ TELETHON: {e_auth}")
            raise
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
        final_error_message = f"!!! ОШИБКА АВТОРИЗАЦИИ в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {e_auth_final}. Таск НЕ будет повторен."
        print(final_error_message)
        raise e_auth_final from e_auth_final
    except Exception as e_task_level:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}"
        print(final_error_message)
        traceback.print_exc()
        try:
            countdown = int(self.default_retry_delay * (2 ** self.request.retries))
            print(f"Попытка retry ({self.request.retries + 1}/{self.max_retries}) для таска {self.request.id} через {countdown} сек из-за {type(e_task_level).__name__}")
            raise self.retry(exc=e_task_level, countdown=countdown)
        except self.MaxRetriesExceededError:
            print(f"Достигнуто максимальное количество попыток для таска {self.request.id}. Ошибка: {e_task_level}")
            raise e_task_level from e_task_level
        except Exception as e_retry_logic:
             print(f"Ошибка в логике retry: {e_retry_logic}")
             raise e_task_level from e_task_level

# --- Остальные задачи (summarize_top_posts_task, send_daily_digest_task, analyze_posts_sentiment_task) ---
# В них также были добавлены фильтры по Channel.is_active == True при выборке постов.
# Их код остается таким же, как в предыдущем ответе, где мы обновляли app/tasks.py.
# Я их здесь опущу для краткости, но они должны быть в файле.

# --- ЗАДАЧА СУММАРИЗАЦИИ (с фильтром по активным каналам) ---
@celery_instance.task(name="summarize_top_posts", bind=True, max_retries=2, default_retry_delay=300)
def summarize_top_posts_task(self, hours_ago=48, top_n=3):
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
        print(error_msg); raise self.retry(exc=e_openai_init)

    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic_summarizer():
        local_async_engine = None; processed_posts_count = 0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)
            async with LocalAsyncSessionFactory() as db_session:
                time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
                active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery()
                stmt_posts_to_summarize = (
                    select(Post)
                    .join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id)
                    .where(Post.posted_at >= time_threshold, Post.summary_text == None)
                    .order_by(desc(Post.comments_count)).limit(top_n)
                )
                result_posts = await db_session.execute(stmt_posts_to_summarize)
                posts_to_process = result_posts.scalars().all()
                if not posts_to_process: print(f"  Не найдено постов для суммаризации из активных каналов."); return "Нет постов для суммаризации."
                print(f"  Найдено {len(posts_to_process)} постов для суммаризации из активных каналов.")
                for post_obj in posts_to_process:
                    if not post_obj.text_content or len(post_obj.text_content.strip()) < 50: continue
                    print(f"    Суммаризация поста ID {post_obj.id} (TG ID: {post_obj.telegram_post_id})...")
                    try:
                        summary_prompt = f"Контекст: AI-аналитик для Telegram. Задача: резюме (1-3 предл. на русском) основной мысли. Текст поста:\n---\n{post_obj.text_content[:4000]}\n---\nРезюме:"
                        completion = await asyncio.to_thread(openai_client.chat.completions.create, model="gpt-3.5-turbo", messages=[{"role": "system", "content": "AI-ассистент для кратких резюме."}, {"role": "user", "content": summary_prompt}], temperature=0.3, max_tokens=150)
                        summary = completion.choices[0].message.content.strip()
                        if summary:
                            post_obj.summary_text = summary; post_obj.updated_at = datetime.now(timezone.utc)
                            db_session.add(post_obj); processed_posts_count += 1
                            print(f"      Резюме для поста ID {post_obj.id} получено.")
                    except OpenAIError as e: print(f"    !!! Ошибка OpenAI API при суммаризации поста ID {post_obj.id}: {e}")
                    except Exception as e: print(f"    !!! Неожиданная ошибка при суммаризации поста ID {post_obj.id}: {e}"); traceback.print_exc(limit=1)
                if processed_posts_count > 0: await db_session.commit(); print(f"  Успешно суммаризировано {processed_posts_count} постов.")
            return f"Суммаризация завершена. Обработано: {processed_posts_count} постов."
        except Exception as e: print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_summarizer: {e}"); traceback.print_exc(); raise
        finally:
            if local_async_engine: await local_async_engine.dispose()
    try:
        result_message = asyncio.run(_async_main_logic_summarizer())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e:
        task_duration = time.time() - task_start_time
        print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {e}")
        traceback.print_exc(); raise self.retry(exc=e)


# --- ЗАДАЧА ОТПРАВКИ ДАЙДЖЕСТА (с фильтром по активным каналам) ---
@celery_instance.task(name="send_daily_digest", bind=True, max_retries=3, default_retry_delay=180)
def send_daily_digest_task(self, hours_ago_posts=24, top_n_summarized=3):
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Отправка дайджеста)...")
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_TARGET_CHAT_ID: print("Ошибка: TELEGRAM_BOT_TOKEN или CHAT_ID не настроены."); return "Config error"
    async def _async_send_digest_logic():
        bot = telegram.Bot(token=settings.TELEGRAM_BOT_TOKEN)
        ASYNC_DB_URL_FOR_TASK_DIGEST = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
        local_async_engine_digest = None; message_parts = []
        try:
            local_async_engine_digest = create_async_engine(ASYNC_DB_URL_FOR_TASK_DIGEST, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactoryDigest = sessionmaker(bind=local_async_engine_digest, class_=AsyncSession, expire_on_commit=False)
            async with LocalAsyncSessionFactoryDigest() as db_session:
                time_threshold_posts = datetime.now(timezone.utc) - timedelta(hours=hours_ago_posts)
                active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery()
                stmt_new_posts_count = select(func.count(Post.id)).join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id).where(Post.posted_at >= time_threshold_posts)
                new_posts_count = (await db_session.execute(stmt_new_posts_count)).scalar_one_or_none() or 0
                message_parts.append(helpers.escape_markdown(f" digest за {hours_ago_posts}ч:\n📰 Новых постов: *{new_posts_count}*\n", version=2))
                stmt_top_posts = (select(Post.link, Post.comments_count, Post.summary_text, Post.post_sentiment_label, Channel.title.label("channel_title")).join(Channel, Post.channel_id == Channel.id).where(Channel.is_active == True, Post.posted_at >= time_threshold_posts, Post.comments_count > 0, Post.summary_text != None).order_by(desc(Post.comments_count)).limit(top_n_summarized))
                top_posts_data = (await db_session.execute(stmt_top_posts)).all()
                if top_posts_data:
                    message_parts.append(helpers.escape_markdown(f"\n🔥 Топ-{len(top_posts_data)} постов:\n", version=2))
                    for i, pd in enumerate(top_posts_data):
                        s_str = ""
                        if pd.post_sentiment_label: s_str = helpers.escape_markdown(f"   {'😊' if pd.post_sentiment_label=='positive' else '😠' if pd.post_sentiment_label=='negative' else '😐' if pd.post_sentiment_label=='neutral' else '🤔'}Тон: {pd.post_sentiment_label.capitalize()}\n", version=2)
                        message_parts.append(f"\n*{helpers.escape_markdown(str(i+1),version=2)}\\.* {helpers.escape_markdown(pd.channel_title or '', version=2)} [{helpers.escape_markdown('Пост',version=2)}]({helpers.escape_markdown(pd.link or '#',version=2)})\n   💬 {helpers.escape_markdown(str(pd.comments_count),version=2)}\n{s_str}   📝 _{helpers.escape_markdown(pd.summary_text or '',version=2)}_\n")
                await bot.send_message(chat_id=settings.TELEGRAM_TARGET_CHAT_ID, text="".join(message_parts), parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
            return f"Дайджест отправлен. Постов: {new_posts_count}, Топ: {len(top_posts_data) if top_posts_data else 0}."
        except Exception as e: print(f"!!! Ошибка в _async_send_digest_logic: {e}"); traceback.print_exc(); raise
        finally:
            if local_async_engine_digest: await local_async_engine_digest.dispose()
    try:
        result_message = asyncio.run(_async_send_digest_logic())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e:
        task_duration = time.time() - task_start_time
        print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {e}");
        traceback.print_exc(); raise self.retry(exc=e)

# --- ЗАДАЧА АНАЛИЗА ТОНАЛЬНОСТИ (с фильтром по активным каналам) ---
@celery_instance.task(name="analyze_posts_sentiment", bind=True, max_retries=2, default_retry_delay=300)
def analyze_posts_sentiment_task(self, limit_posts_to_analyze=5):
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Анализ тональности, лимит: {limit_posts_to_analyze})...")
    if not settings.OPENAI_API_KEY: print("Ошибка: OPENAI_API_KEY не настроен."); return "Config error"
    try: openai_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    except Exception as e: print(f"Ошибка OpenAI init: {e}"); raise self.retry(exc=e)
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
    async def _async_main_logic_sentiment_analyzer():
        local_async_engine = None; analyzed_posts_count = 0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)
            async with LocalAsyncSessionFactory() as db_session:
                active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery()
                stmt_posts_to_analyze = (select(Post).join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id).where(Post.text_content != None, Post.text_content != '', Post.post_sentiment_label == None).order_by(Post.posted_at.asc()).limit(limit_posts_to_analyze))
                posts_to_process = (await db_session.execute(stmt_posts_to_analyze)).scalars().all()
                if not posts_to_process: print(f"  Не найдено постов для анализа тональности из активных каналов."); return "Нет постов для анализа."
                print(f"  Найдено {len(posts_to_process)} постов для анализа из активных каналов.")
                for post_obj in posts_to_process:
                    if not post_obj.text_content: continue
                    print(f"    Анализ тональности поста ID {post_obj.id}...")
                    s_label, s_score = "neutral", 0.0
                    try:
                        prompt = f"Определи тональность текста (JSON: sentiment_label: [positive,negative,neutral,mixed], sentiment_score: [-1.0,1.0]):\n---\n{post_obj.text_content[:3500]}\n---\nJSON_RESPONSE:"
                        completion = await asyncio.to_thread(openai_client.chat.completions.create, model="gpt-3.5-turbo", messages=[{"role": "system", "content": "AI-аналитик тональности (JSON)."}, {"role": "user", "content": prompt}], temperature=0.2, max_tokens=50, response_format={"type": "json_object"})
                        raw_resp = completion.choices[0].message.content
                        if raw_resp:
                            try:
                                data = json.loads(raw_resp)
                                s_label = data.get("sentiment_label", "neutral")
                                s_score = float(data.get("sentiment_score", 0.0))
                                if s_label not in ["positive","negative","neutral","mixed"]: s_label="neutral"
                                if not (-1.0 <= s_score <= 1.0): s_score=0.0
                            except (json.JSONDecodeError, TypeError, ValueError): print(f"  Ошибка парсинга JSON от LLM: {raw_resp}")
                    except OpenAIError as e: print(f"    !!! Ошибка OpenAI API для поста ID {post_obj.id}: {e}"); continue
                    except Exception as e: print(f"    !!! Неожиданная ошибка анализа поста ID {post_obj.id}: {e}"); traceback.print_exc(limit=1); continue
                    post_obj.post_sentiment_label = s_label; post_obj.post_sentiment_score = s_score
                    post_obj.updated_at = datetime.now(timezone.utc); db_session.add(post_obj); analyzed_posts_count += 1
                    print(f"      Тональность поста ID {post_obj.id}: {s_label} ({s_score:.2f})")
                if analyzed_posts_count > 0: await db_session.commit(); print(f"  Проанализировано {analyzed_posts_count} постов.")
            return f"Анализ тональности завершен. Обработано: {analyzed_posts_count} постов."
        except Exception as e: print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_sentiment_analyzer: {e}"); traceback.print_exc(); raise
        finally:
            if local_async_engine: await local_async_engine.dispose()
    try:
        result_message = asyncio.run(_async_main_logic_sentiment_analyzer())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) успешно завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e:
        task_duration = time.time() - task_start_time
        print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в Celery таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {e}")
        traceback.print_exc(); raise self.retry(exc=e)