# app/tasks.py

import asyncio
import os
import time
import traceback
import json
from datetime import timezone, datetime, timedelta
from typing import List, Dict, Any, Optional # Добавил typing

import openai
from openai import OpenAIError

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy import desc, func, update

import telegram # Для python-telegram-bot (дайджесты)
from telegram.constants import ParseMode
from telegram import helpers

from telethon.errors import (
    FloodWaitError, ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError,
    MessageIdInvalidError as TelethonMessageIdInvalidError # Переименовал для ясности
)
from telethon.tl.types import (
    Message, User as TelethonUserType, Channel as TelethonChannelType, Chat as TelethonChatType,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaPoll, MessageMediaWebPage,
    MessageMediaGame, MessageMediaInvoice, MessageMediaGeo, MessageMediaContact, MessageMediaDice,
    MessageMediaUnsupported, MessageMediaEmpty, Poll, PollAnswer, ReactionCount, ReactionEmoji, ReactionCustomEmoji,
    MessageReplies, PeerUser, PeerChat, PeerChannel, MessageReplyHeader,
    DocumentAttributeFilename, DocumentAttributeAnimated, DocumentAttributeVideo, DocumentAttributeAudio,
    WebPage, WebPageEmpty, MessageService # Добавил MessageService для пропуска
)
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

# --- ОСНОВНАЯ ОБНОВЛЕННАЯ ЗАДАЧА СБОРА ДАННЫХ ---
@celery_instance.task(name="collect_telegram_data", bind=True, max_retries=3, default_retry_delay=60 * 5)
def collect_telegram_data_task(self):
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (РАСШИРЕННЫЙ сбор постов и комментариев из БД)...")

    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    if not all([api_id_val, api_hash_val, phone_number_val]):
        error_msg = "Ошибка: Telegram API credentials не настроены."
        print(error_msg)
        return error_msg
    
    session_file_path_in_container = "/app/celery_telegram_session" # Убедимся, что это правильное имя
    print(f"Celery Worker использует Telethon session: {session_file_path_in_container}.session")
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _process_media_for_db(message_media: Any) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
        media_type_str: Optional[str] = None
        media_info_dict: Dict[str, Any] = {}

        if not message_media or isinstance(message_media, MessageMediaEmpty):
            return None, None

        if isinstance(message_media, MessageMediaPhoto):
            media_type_str = "photo"
            if message_media.photo and hasattr(message_media.photo, 'id'):
                 media_info_dict['id'] = message_media.photo.id
            if hasattr(message_media, 'ttl_seconds') and message_media.ttl_seconds: # Для самоуничтожающихся фото
                media_info_dict['ttl_seconds'] = message_media.ttl_seconds
        
        elif isinstance(message_media, MessageMediaDocument):
            doc = message_media.document
            media_type_str = "document" # Общий тип по умолчанию
            if hasattr(doc, 'id'): media_info_dict['id'] = doc.id
            if hasattr(doc, 'mime_type'): media_info_dict['mime_type'] = doc.mime_type
            
            filename = next((attr.file_name for attr in doc.attributes if isinstance(attr, DocumentAttributeFilename)), None)
            if filename: media_info_dict['filename'] = filename
            
            duration = next((attr.duration for attr in doc.attributes if isinstance(attr, (DocumentAttributeAudio, DocumentAttributeVideo))), None)
            if duration is not None: media_info_dict['duration'] = float(duration) # Убедимся, что это float

            # Более точное определение типа
            is_gif = any(isinstance(attr, DocumentAttributeAnimated) for attr in doc.attributes)
            is_video = any(isinstance(attr, DocumentAttributeVideo) and not getattr(attr, 'round_message', False) for attr in doc.attributes)
            is_audio = any(isinstance(attr, DocumentAttributeAudio) and not getattr(attr, 'voice', False) for attr in doc.attributes)
            is_voice = any(isinstance(attr, DocumentAttributeAudio) and getattr(attr, 'voice', False) for attr in doc.attributes)
            is_video_note = any(isinstance(attr, DocumentAttributeVideo) and getattr(attr, 'round_message', False) for attr in doc.attributes)

            if is_gif: media_type_str = "gif"
            elif is_video_note: media_type_str = "video_note" # Кружочек
            elif is_voice: media_type_str = "voice" # Голосовое сообщение
            elif is_video: media_type_str = "video"
            elif is_audio: media_type_str = "audio"
            # Если это просто документ, тип остается "document"
        
        elif isinstance(message_media, MessageMediaPoll):
            media_type_str = "poll"
            poll: Poll = message_media.poll
            # Преобразуем TextWithEntities в строку
            media_info_dict['question'] = str(poll.question.text) if hasattr(poll.question, 'text') else str(poll.question)
            media_info_dict['answers'] = [{'text': str(ans.text), 'option': ans.option.decode('utf-8','replace')} for ans in poll.answers]
            media_info_dict['closed'] = poll.closed
            media_info_dict['quiz'] = poll.quiz
            if message_media.results and message_media.results.total_voters is not None:
                media_info_dict['total_voters'] = message_media.results.total_voters
                if message_media.results.results:
                    media_info_dict['results_summary'] = [
                        {'option': ans.option.decode('utf-8','replace'), 'voters': ans.voters, 'correct': getattr(ans, 'correct', None)} 
                        for ans in message_media.results.results
                    ]
        
        elif isinstance(message_media, MessageMediaWebPage):
            media_type_str = "webpage"
            wp: WebPage | WebPageEmpty = message_media.webpage
            if isinstance(wp, WebPage):
                media_info_dict['url'] = wp.url
                media_info_dict['display_url'] = wp.display_url
                media_info_dict['type'] = wp.type
                media_info_dict['site_name'] = str(wp.site_name) if wp.site_name else None # Преобразуем TextWithEntities
                media_info_dict['title'] = str(wp.title) if wp.title else None # Преобразуем TextWithEntities
                media_info_dict['description'] = str(wp.description) if wp.description else None # Преобразуем TextWithEntities
                if wp.photo and hasattr(wp.photo, 'id'): media_info_dict['photo_id'] = wp.photo.id
                if wp.duration: media_info_dict['duration'] = float(wp.duration)
        
        elif isinstance(message_media, MessageMediaGeo) and hasattr(message_media.geo, 'lat') and hasattr(message_media.geo, 'long'):
            media_type_str = "geo"; media_info_dict['latitude'] = message_media.geo.lat; media_info_dict['longitude'] = message_media.geo.long
        elif isinstance(message_media, MessageMediaContact):
            media_type_str = "contact"; media_info_dict['phone_number'] = message_media.phone_number; media_info_dict['first_name'] = message_media.first_name; media_info_dict['last_name'] = message_media.last_name
        elif isinstance(message_media, MessageMediaGame) and message_media.game:
            media_type_str = "game"; media_info_dict['title'] = str(message_media.game.title) if message_media.game.title else None
        elif isinstance(message_media, MessageMediaInvoice) and message_media.title:
            media_type_str = "invoice"; media_info_dict['title'] = str(message_media.title) if message_media.title else None
        elif isinstance(message_media, MessageMediaDice):
            media_type_str = "dice"; media_info_dict['emoticon'] = message_media.emoticon; media_info_dict['value'] = message_media.value
        elif isinstance(message_media, MessageMediaUnsupported):
            media_type_str = "unsupported"
        else: # Если тип не распознан или это MessageMediaEmpty (уже обработан в начале)
            if media_type_str is None: # Если не был установлен ранее
                 media_type_str = f"other_{type(message_media).__name__}"
                 media_info_dict['raw_type'] = type(message_media).__name__


        return media_type_str, media_info_dict if media_info_dict else None

    async def _process_reactions_for_db(message_reactions_attr: Optional[MessageReplies]) -> Optional[List[Dict[str, Any]]]:
        if not message_reactions_attr or not message_reactions_attr.results:
            return None
        
        processed_reactions = []
        for reaction_count_obj in message_reactions_attr.results:
            reaction_count_obj: ReactionCount
            reaction_val = None
            if isinstance(reaction_count_obj.reaction, ReactionEmoji):
                reaction_val = reaction_count_obj.reaction.emoticon
            elif isinstance(reaction_count_obj.reaction, ReactionCustomEmoji):
                reaction_val = f"custom_emoji_{reaction_count_obj.reaction.document_id}"
            
            if reaction_val:
                processed_reactions.append({
                    "reaction": reaction_val,
                    "count": reaction_count_obj.count
                    # "chosen": getattr(reaction_count_obj, 'chosen', False) # Если нужно знать, выбрал ли ее текущий пользователь
                })
        return processed_reactions if processed_reactions else None

    async def _async_main_logic_collector():
        tg_client = None; local_async_engine = None
        total_channels_processed, total_posts_collected, total_comments_collected = 0,0,0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)
            print(f"Celery: Создание TelegramClient с сессией: {session_file_path_in_container}")
            tg_client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val)
            print(f"Celery: Подключение к Telegram..."); await tg_client.connect()
            if not await tg_client.is_user_authorized(): raise ConnectionRefusedError(f"Celery: Пользователь не авторизован для сессии {session_file_path_in_container}.session")
            me = await tg_client.get_me(); print(f"Celery: Успешно подключен как: {me.first_name} (@{me.username or ''})")
            
            async with LocalAsyncSessionFactory() as db_session:
                active_channels_from_db: List[Channel] = (await db_session.execute(select(Channel).where(Channel.is_active == True))).scalars().all()
                if not active_channels_from_db: print("Celery: Нет активных каналов в БД."); return "Нет активных каналов."
                print(f"Celery: Найдено {len(active_channels_from_db)} активных каналов.")

                for channel_db_obj in active_channels_from_db:
                    print(f"\nCelery: Обработка канала: '{channel_db_obj.title}' (ID: {channel_db_obj.id})")
                    total_channels_processed += 1; newly_added_post_objects_in_session: list[Post] = []
                    try:
                        channel_entity_tg = await tg_client.get_entity(channel_db_obj.id)
                        if not isinstance(channel_entity_tg, TelethonChannelType) or not (getattr(channel_entity_tg, 'broadcast', False) or getattr(channel_entity_tg, 'megagroup', False)):
                            print(f"  Канал {channel_db_obj.id} невалиден. Деактивируем."); channel_db_obj.is_active = False; db_session.add(channel_db_obj); continue
                        # Обновление метаданных канала, если нужно (как раньше)

                        iter_messages_params = {"entity": channel_entity_tg, "limit": settings.POST_FETCH_LIMIT}
                        if channel_db_obj.last_processed_post_id: iter_messages_params["min_id"] = channel_db_obj.last_processed_post_id
                        elif settings.INITIAL_POST_FETCH_START_DATETIME: iter_messages_params["offset_date"] = settings.INITIAL_POST_FETCH_START_DATETIME; iter_messages_params["reverse"] = True
                        
                        latest_post_id_seen_this_run = channel_db_obj.last_processed_post_id or 0
                        collected_for_this_channel_this_run = 0
                        temp_posts_buffer_for_db_add: list[Post] = []

                        async for message_tg in tg_client.iter_messages(**iter_messages_params):
                            message_tg: Message
                            if isinstance(message_tg, MessageService) or message_tg.action: continue # Пропускаем служебные сообщения
                            if not (message_tg.text or message_tg.media or message_tg.poll): continue # Пропускаем совсем пустые

                            if message_tg.id > latest_post_id_seen_this_run: latest_post_id_seen_this_run = message_tg.id
                            if (await db_session.execute(select(Post.id).where(Post.telegram_post_id == message_tg.id, Post.channel_id == channel_db_obj.id))).scalar_one_or_none() is not None: continue
                            
                            post_text = None; post_caption = None
                            if message_tg.media and message_tg.text: post_caption = message_tg.text
                            elif not message_tg.media and message_tg.text: post_text = message_tg.text
                                
                            media_type, media_info = await _process_media_for_db(message_tg.media)
                            reactions_data = await _process_reactions_for_db(message_tg.reactions)
                            
                            reply_to_post_id = message_tg.reply_to.reply_to_msg_id if message_tg.reply_to and hasattr(message_tg.reply_to, 'reply_to_msg_id') else None
                            sender_id = message_tg.from_id.user_id if isinstance(message_tg.from_id, PeerUser) else None

                            post_link = f"https://t.me/{channel_db_obj.username or f'c/{channel_db_obj.id}'}/{message_tg.id}"

                            new_post_db_obj = Post(
                                telegram_post_id=message_tg.id, channel_id=channel_db_obj.id, link=post_link,
                                text_content=post_text, views_count=message_tg.views,
                                posted_at=message_tg.date.replace(tzinfo=timezone.utc) if message_tg.date else datetime.now(timezone.utc),
                                reactions=reactions_data, media_type=media_type, media_content_info=media_info, caption_text=post_caption,
                                reply_to_telegram_post_id=reply_to_post_id, forwards_count=message_tg.forwards,
                                author_signature=message_tg.post_author, sender_user_id=sender_id,
                                grouped_id=message_tg.grouped_id, 
                                edited_at=message_tg.edit_date.replace(tzinfo=timezone.utc) if message_tg.edit_date else None,
                                is_pinned=message_tg.pinned or False
                            )
                            temp_posts_buffer_for_db_add.append(new_post_db_obj); newly_added_post_objects_in_session.append(new_post_db_obj)
                            collected_for_this_channel_this_run += 1; total_posts_collected += 1
                        
                        if temp_posts_buffer_for_db_add: db_session.add_all(temp_posts_buffer_for_db_add)
                        if latest_post_id_seen_this_run > (channel_db_obj.last_processed_post_id or 0):
                            channel_db_obj.last_processed_post_id = latest_post_id_seen_this_run; db_session.add(channel_db_obj)
                        
                        # Сбор комментариев
                        if newly_added_post_objects_in_session:
                            print(f"  Celery: Сбор комментариев для {len(newly_added_post_objects_in_session)} новых постов..."); await db_session.flush()
                            comments_collected_channel_total = 0
                            for post_obj_in_db in newly_added_post_objects_in_session:
                                current_post_comm_count = 0
                                try:
                                    async for comment_msg in tg_client.iter_messages(entity=channel_entity_tg, limit=settings.COMMENT_FETCH_LIMIT, reply_to=post_obj_in_db.telegram_post_id):
                                        comment_msg: Message
                                        if comment_msg.action or not (comment_msg.text or comment_msg.media or comment_msg.poll): continue
                                        if (await db_session.execute(select(Comment.id).where(Comment.telegram_comment_id == comment_msg.id, Comment.post_id == post_obj_in_db.id))).scalar_one_or_none() is not None: continue

                                        comm_text = None; comm_caption = None
                                        if comment_msg.media and comment_msg.text: comm_caption = comment_msg.text
                                        elif not comment_msg.media and comment_msg.text: comm_text = comment_msg.text
                                        
                                        comm_media_type, comm_media_info = await _process_media_for_db(comment_msg.media)
                                        comm_reactions = await _process_reactions_for_db(comment_msg.reactions)
                                        comm_reply_to_id = comment_msg.reply_to.reply_to_msg_id if comment_msg.reply_to and hasattr(comment_msg.reply_to, 'reply_to_msg_id') else None
                                        
                                        comm_user_id, comm_user_username, comm_user_fullname = None, None, None
                                        if isinstance(comment_msg.from_id, PeerUser):
                                            comm_user_id = comment_msg.from_id.user_id
                                            # Опционально: получить детали юзера, но аккуратно с FloodWait
                                            # try: sender_entity = await tg_client.get_entity(comm_user_id); ... except: pass
                                        
                                        new_comm_obj = Comment(
                                            telegram_comment_id=comment_msg.id, post_id=post_obj_in_db.id,
                                            telegram_user_id=comm_user_id, user_username=comm_user_username, user_fullname=comm_user_fullname,
                                            text_content=comm_text or "", # Гарантируем не None
                                            commented_at=comment_msg.date.replace(tzinfo=timezone.utc) if comment_msg.date else datetime.now(timezone.utc),
                                            reactions=comm_reactions, reply_to_telegram_comment_id=comm_reply_to_id,
                                            media_type=comm_media_type, media_content_info=comm_media_info, caption_text=comm_caption,
                                            edited_at=comment_msg.edit_date.replace(tzinfo=timezone.utc) if comment_msg.edit_date else None
                                        )
                                        db_session.add(new_comm_obj)
                                        current_post_comm_count +=1; total_comments_collected +=1
                                    if current_post_comm_count > 0:
                                        await db_session.execute(update(Post).where(Post.id == post_obj_in_db.id).values(comments_count=(Post.comments_count if Post.comments_count is not None else 0) + current_post_comm_count))
                                        comments_collected_channel_total += current_post_comm_count
                                except TelethonMessageIdInvalidError: pass # Комментарии могут быть удалены/недоступны
                                except Exception as e_comm_loop: print(f"Ошибка в цикле сбора комм для поста {post_obj_in_db.telegram_post_id}: {e_comm_loop}")
                            if comments_collected_channel_total > 0 : print(f"    Собрано {comments_collected_channel_total} комментариев для канала.")
                    # app/tasks.py
# ... (начало функции _async_main_logic_collector и цикл по каналам) ...
                    # ... (внутри цикла for channel_db_obj in active_channels_from_db:)
                    # ... (вся логика сбора для одного канала) ...
                    except Exception as e_ch_proc: 
                        print(f"  Celery: Ошибка обработки канала '{channel_db_obj.title}': {type(e_ch_proc).__name__} - {e_ch_proc}")
                        traceback.print_exc(limit=1) # Печатаем traceback для этой конкретной ошибки канала
                
                # --- ГЛАВНЫЙ КОММИТ ПОСЛЕ ОБРАБОТКИ ВСЕХ КАНАЛОВ ---
                await db_session.commit() 
                print(f"\nCelery: Все изменения (посты, комментарии, каналы) сохранены в БД. Обработано: {total_channels_processed} каналов.")
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---

            # Этот return для try-блока внутри async with LocalAsyncSessionFactory()
            return f"Celery: Сбор данных завершен успешно. Статистика: {total_channels_processed}ch, {total_posts_collected}p, {total_comments_collected}c."
        
        except ConnectionRefusedError as e_auth: # Ошибка авторизации Telethon
            print(f"!!! Celery: ОШИБКА АВТОРИЗАЦИИ TELETHON: {e_auth}")
            raise # Передаем выше, чтобы Celery НЕ повторял задачу из-за этого (если нет смысла)
        
        except Exception as e_async_logic: # Любая другая ошибка внутри основной логики _async_main_logic_collector
            print(f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА внутри _async_main_logic_collector: {type(e_async_logic).__name__} {e_async_logic}")
            traceback.print_exc()
            # Здесь важно решить, нужно ли откатывать транзакцию, если она еще активна.
            # Но так как мы уже вышли из блока `async with db_session`, сессия должна быть закрыта.
            # Если ошибка произошла до финального commit(), то ничего не сохранится, что в данном случае может быть приемлемо.
            raise # Передаем выше, чтобы Celery мог попытаться сделать retry для всей задачи
        
        finally: # Этот блок выполнится всегда
            if tg_client and tg_client.is_connected():
                print("Celery: Отключение Telegram клиента из _async_main_logic_collector (finally)...")
                await tg_client.disconnect()
            if local_async_engine:
                print("Celery: Закрытие пула соединений БД (local_async_engine) из _async_main_logic_collector (finally)...")
                await local_async_engine.dispose()

    # Это основной try/except/finally для всей Celery-задачи (синхронной обертки)
    try:
        result_message = asyncio.run(_async_main_logic_collector())
        task_duration = time.time() - task_start_time
        print(f"Celery таск '{self.name}' (ID: {self.request.id}) УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except ConnectionRefusedError as e_auth_final:
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! Celery: ОШИБКА АВТОРИЗАЦИИ в таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {e_auth_final}. Таск НЕ будет повторен."
        print(final_error_message)
        raise e_auth_final from e_auth_final # Просто пробрасываем, Celery ее обработает как failed
    except Exception as e_task_level: # Все остальные ошибки могут быть временными
        task_duration = time.time() - task_start_time
        final_error_message = f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (ID: {self.request.id}) (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}"
        print(final_error_message)
        traceback.print_exc()
        try:
            # Пытаемся перезапустить задачу с экспоненциальной задержкой
            countdown = int(self.default_retry_delay * (2 ** self.request.retries))
            print(f"Celery: Попытка retry ({self.request.retries + 1}/{self.max_retries}) для таска {self.request.id} через {countdown} сек из-за {type(e_task_level).__name__}")
            raise self.retry(exc=e_task_level, countdown=countdown) # Используем self.retry
        except self.MaxRetriesExceededError: # self.MaxRetriesExceededError из celery.exceptions
            print(f"Celery: Достигнуто максимальное количество попыток для таска {self.request.id}. Ошибка: {e_task_level}")
            raise e_task_level from e_task_level # Передаем выше как окончательный провал
        except Exception as e_retry_logic: # Ловим ошибки в самой логике retry, если они есть
             print(f"Celery: Ошибка в логике retry: {e_retry_logic}")
             raise e_task_level from e_task_level # Передаем оригинальную ошибку, так как retry не сработал

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