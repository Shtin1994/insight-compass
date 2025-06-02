# app/tasks.py

import asyncio
import os
import time
import traceback
import json
from datetime import timezone, datetime, timedelta
from typing import List, Dict, Any, Optional

import openai # Оставляем, так как используется в summarize_top_posts_task и analyze_posts_sentiment_task
from openai import OpenAIError # Оставляем

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.future import select
from sqlalchemy import desc, func, update, cast, literal_column, nullslast, Integer as SAInteger
from sqlalchemy.dialects.postgresql import JSONB

import telegram # Оставляем для send_daily_digest_task
from telegram.constants import ParseMode # Оставляем
from telegram import helpers # Оставляем

from telethon.errors import (
    FloodWaitError, ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError,
    MessageIdInvalidError as TelethonMessageIdInvalidError
)
from telethon.tl.types import (
    Message, User as TelethonUserType, Channel as TelethonChannelType, Chat as TelethonChatType,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaPoll, MessageMediaWebPage,
    MessageMediaGame, MessageMediaInvoice, MessageMediaGeo, MessageMediaContact, MessageMediaDice,
    MessageMediaUnsupported, MessageMediaEmpty, Poll, PollAnswer, ReactionCount, ReactionEmoji, ReactionCustomEmoji,
    MessageReplies, PeerUser, PeerChat, PeerChannel, MessageReplyHeader,
    DocumentAttributeFilename, DocumentAttributeAnimated, DocumentAttributeVideo, DocumentAttributeAudio,
    WebPage, WebPageEmpty, MessageService
)
from telethon import TelegramClient

from app.celery_app import celery_instance
from app.core.config import settings
from app.models.telegram_data import Channel, Post, Comment # Убедимся, что Comment импортирован

# --- НАЧАЛО: Импорт для новых задач ---
from app.db.session import get_async_session_context_manager # Предполагаем, что этот менеджер контекста существует
# Предполагаем, что у вас есть сервис для LLM
try:
    from app.services.llm_service import одиночный_запрос_к_llm 
except ImportError:
    # Заглушка, если сервис еще не создан, чтобы файл мог импортироваться
    async def одиночный_запрос_к_llm(prompt: str, модель: str, **kwargs) -> Optional[str]:
        print(f"ЗАГЛУШКА: одиночный_запрос_к_llm вызван с промтом: {prompt[:100]}...")
        # Для тестирования можно возвращать фейковый JSON
        # return json.dumps({
        #     "topics": ["тест_тема"], "problems": [], "questions": ["тестовый вопрос?"], "suggestions": []
        # })
        return None
# --- КОНЕЦ: Импорт для новых задач ---

# Настройка логгера для задач Celery (если еще не настроено глобально)
logger = celery_instance.log.get_default_logger()


# --- Существующие задачи (add, simple_debug_task) ---
@celery_instance.task(name="add")
def add(x, y): logger.info(f"Тестовый таск 'add': {x} + {y}"); time.sleep(5); result = x + y; logger.info(f"Результат 'add': {result}"); return result
@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str): logger.info(f"Тестовый таск 'simple_debug_task' получил: {message}"); time.sleep(3); return f"Сообщение '{message}' обработано"

# --- Вспомогательные функции для collect_telegram_data_task ---
# (остаются без изменений, как в вашем коде)
async def _process_media_for_db(message_media: Any) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
    media_type_str: Optional[str] = None; media_info_dict: Dict[str, Any] = {}
    if not message_media or isinstance(message_media, MessageMediaEmpty): return None, None
    if isinstance(message_media, MessageMediaPhoto):
        media_type_str = "photo";
        if message_media.photo and hasattr(message_media.photo, 'id'): media_info_dict['id'] = message_media.photo.id
        if hasattr(message_media, 'ttl_seconds') and message_media.ttl_seconds: media_info_dict['ttl_seconds'] = message_media.ttl_seconds
    elif isinstance(message_media, MessageMediaDocument):
        doc = message_media.document; media_type_str = "document"
        if hasattr(doc, 'id'): media_info_dict['id'] = doc.id
        if hasattr(doc, 'mime_type'): media_info_dict['mime_type'] = doc.mime_type
        filename = next((attr.file_name for attr in doc.attributes if isinstance(attr, DocumentAttributeFilename)), None)
        if filename: media_info_dict['filename'] = filename
        duration = next((attr.duration for attr in doc.attributes if isinstance(attr, (DocumentAttributeAudio, DocumentAttributeVideo))), None)
        if duration is not None: media_info_dict['duration'] = float(duration)
        is_gif = any(isinstance(attr, DocumentAttributeAnimated) for attr in doc.attributes); is_video = any(isinstance(attr, DocumentAttributeVideo) and not getattr(attr, 'round_message', False) for attr in doc.attributes); is_audio = any(isinstance(attr, DocumentAttributeAudio) and not getattr(attr, 'voice', False) for attr in doc.attributes); is_voice = any(isinstance(attr, DocumentAttributeAudio) and getattr(attr, 'voice', False) for attr in doc.attributes); is_video_note = any(isinstance(attr, DocumentAttributeVideo) and getattr(attr, 'round_message', False) for attr in doc.attributes)
        if is_gif: media_type_str = "gif"
        elif is_video_note: media_type_str = "video_note"
        elif is_voice: media_type_str = "voice"
        elif is_video: media_type_str = "video"
        elif is_audio: media_type_str = "audio"
    elif isinstance(message_media, MessageMediaPoll):
        media_type_str = "poll"; poll: Poll = message_media.poll
        media_info_dict['question'] = str(poll.question.text) if hasattr(poll.question, 'text') and poll.question.text is not None else str(poll.question)
        media_info_dict['answers'] = [{'text': str(ans.text), 'option': ans.option.decode('utf-8','replace')} for ans in poll.answers]
        media_info_dict['closed'] = poll.closed; media_info_dict['quiz'] = poll.quiz
        if message_media.results and message_media.results.total_voters is not None:
            media_info_dict['total_voters'] = message_media.results.total_voters
            if message_media.results.results: media_info_dict['results_summary'] = [{'option': ans.option.decode('utf-8','replace'), 'voters': ans.voters, 'correct': getattr(ans, 'correct', None)} for ans in message_media.results.results]
    elif isinstance(message_media, MessageMediaWebPage):
        media_type_str = "webpage"; wp: WebPage | WebPageEmpty = message_media.webpage
        if isinstance(wp, WebPage): 
            media_info_dict['url'] = wp.url; media_info_dict['display_url'] = wp.display_url; media_info_dict['type'] = wp.type
            media_info_dict['site_name'] = str(wp.site_name) if wp.site_name else None
            media_info_dict['title'] = str(wp.title) if wp.title else None
            media_info_dict['description'] = str(wp.description) if wp.description else None
            if wp.photo and hasattr(wp.photo, 'id'): media_info_dict['photo_id'] = wp.photo.id
            if wp.duration: media_info_dict['duration'] = float(wp.duration)
    elif isinstance(message_media, MessageMediaGeo) and hasattr(message_media.geo, 'lat') and hasattr(message_media.geo, 'long'): media_type_str = "geo"; media_info_dict['latitude'] = message_media.geo.lat; media_info_dict['longitude'] = message_media.geo.long
    elif isinstance(message_media, MessageMediaContact): media_type_str = "contact"; media_info_dict['phone_number'] = message_media.phone_number; media_info_dict['first_name'] = message_media.first_name; media_info_dict['last_name'] = message_media.last_name
    elif isinstance(message_media, MessageMediaGame) and message_media.game: media_type_str = "game"; media_info_dict['title'] = str(message_media.game.title) if message_media.game.title else None
    elif isinstance(message_media, MessageMediaInvoice) and message_media.title: media_type_str = "invoice"; media_info_dict['title'] = str(message_media.title) if message_media.title else None
    elif isinstance(message_media, MessageMediaDice): media_type_str = "dice"; media_info_dict['emoticon'] = message_media.emoticon; media_info_dict['value'] = message_media.value
    elif isinstance(message_media, MessageMediaUnsupported): media_type_str = "unsupported"
    else:
        if media_type_str is None: media_type_str = f"other_{type(message_media).__name__}"; media_info_dict['raw_type'] = type(message_media).__name__
    return media_type_str, media_info_dict if media_info_dict else None

async def _process_reactions_for_db(message_reactions_attr: Optional[MessageReplies]) -> Optional[List[Dict[str, Any]]]:
    if not message_reactions_attr or not message_reactions_attr.results: return None
    processed_reactions = []
    for reaction_count_obj in message_reactions_attr.results:
        reaction_count_obj: ReactionCount; reaction_val = None
        if isinstance(reaction_count_obj.reaction, ReactionEmoji): reaction_val = reaction_count_obj.reaction.emoticon
        elif isinstance(reaction_count_obj.reaction, ReactionCustomEmoji): reaction_val = f"custom_emoji_{reaction_count_obj.reaction.document_id}"
        if reaction_val: processed_reactions.append({"reaction": reaction_val, "count": reaction_count_obj.count})
    return processed_reactions if processed_reactions else None

# --- ОСНОВНАЯ ЗАДАЧА СБОРА ДАННЫХ ---
# (остается без изменений, как в вашем коде)
@celery_instance.task(name="collect_telegram_data", bind=True, max_retries=3, default_retry_delay=60 * 5)
def collect_telegram_data_task(self):
    task_start_time = time.time(); logger.info(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (РАСШИРЕННЫЙ сбор)...")
    api_id_val = settings.TELEGRAM_API_ID; api_hash_val = settings.TELEGRAM_API_HASH; phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    if not all([api_id_val, api_hash_val, phone_number_val]): logger.error("Ошибка: Telegram API credentials не настроены."); return "Config error"
    session_file_path_in_container = "/app/celery_telegram_session"; logger.info(f"Celery Worker session: {session_file_path_in_container}.session")
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL_FOR_ALEMBIC.replace("postgresql://", "postgresql+asyncpg://") if settings.DATABASE_URL_FOR_ALEMBIC else settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_main_logic_collector():
        tg_client = None; local_async_engine = None; total_channels_processed, total_posts_collected, total_comments_collected = 0,0,0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)
            logger.info(f"Celery: Создание TGClient: {session_file_path_in_container}"); tg_client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val)
            logger.info(f"Celery: Подключение к TG..."); await tg_client.connect()
            if not await tg_client.is_user_authorized(): raise ConnectionRefusedError(f"Celery: Пользователь не авторизован для {session_file_path_in_container}.session")
            me = await tg_client.get_me(); logger.info(f"Celery: Успешно подключен как: {me.first_name} (@{me.username or ''})")
            async with LocalAsyncSessionFactory() as db_session:
                active_channels_from_db: List[Channel] = (await db_session.execute(select(Channel).where(Channel.is_active == True))).scalars().all()
                if not active_channels_from_db: logger.info("Celery: Нет активных каналов."); return "Нет активных каналов."
                logger.info(f"Celery: Найдено {len(active_channels_from_db)} активных каналов.")
                for channel_db_obj in active_channels_from_db:
                    logger.info(f"\nCelery: Обработка канала: '{channel_db_obj.title}' (ID: {channel_db_obj.id})"); total_channels_processed += 1
                    newly_added_post_objects_in_session: list[Post] = []
                    try:
                        channel_entity_tg = await tg_client.get_entity(channel_db_obj.id)
                        if not isinstance(channel_entity_tg, TelethonChannelType) or not (getattr(channel_entity_tg, 'broadcast', False) or getattr(channel_entity_tg, 'megagroup', False)):
                            logger.warning(f"  Канал {channel_db_obj.id} невалиден. Деактивируем."); channel_db_obj.is_active = False; db_session.add(channel_db_obj); continue
                        iter_messages_params = {"entity": channel_entity_tg, "limit": settings.POST_FETCH_LIMIT}
                        if channel_db_obj.last_processed_post_id: iter_messages_params["min_id"] = channel_db_obj.last_processed_post_id
                        elif settings.INITIAL_POST_FETCH_START_DATETIME: iter_messages_params["offset_date"] = settings.INITIAL_POST_FETCH_START_DATETIME; iter_messages_params["reverse"] = True
                        latest_post_id_seen_this_run = channel_db_obj.last_processed_post_id or 0; collected_for_this_channel_this_run = 0; temp_posts_buffer_for_db_add: list[Post] = []
                        async for message_tg in tg_client.iter_messages(**iter_messages_params):
                            message_tg: Message
                            if isinstance(message_tg, MessageService) or message_tg.action: continue
                            if not (message_tg.text or message_tg.media or message_tg.poll): continue
                            if message_tg.id > latest_post_id_seen_this_run: latest_post_id_seen_this_run = message_tg.id
                            if (await db_session.execute(select(Post.id).where(Post.telegram_post_id == message_tg.id, Post.channel_id == channel_db_obj.id))).scalar_one_or_none() is not None: continue
                            post_text = None; post_caption = None
                            if message_tg.media and message_tg.text: post_caption = message_tg.text
                            elif not message_tg.media and message_tg.text: post_text = message_tg.text
                            media_type, media_info = await _process_media_for_db(message_tg.media); reactions_data = await _process_reactions_for_db(message_tg.reactions)
                            reply_to_post_id = message_tg.reply_to.reply_to_msg_id if message_tg.reply_to and hasattr(message_tg.reply_to, 'reply_to_msg_id') else None
                            sender_id = message_tg.from_id.user_id if isinstance(message_tg.from_id, PeerUser) else None
                            post_link = f"https://t.me/{channel_db_obj.username or f'c/{channel_db_obj.id}'}/{message_tg.id}"
                            new_post_db_obj = Post(telegram_post_id=message_tg.id, channel_id=channel_db_obj.id, link=post_link, text_content=post_text, views_count=message_tg.views, posted_at=message_tg.date.replace(tzinfo=timezone.utc) if message_tg.date else datetime.now(timezone.utc), reactions=reactions_data, media_type=media_type, media_content_info=media_info, caption_text=post_caption, reply_to_telegram_post_id=reply_to_post_id, forwards_count=message_tg.forwards, author_signature=message_tg.post_author, sender_user_id=sender_id, grouped_id=message_tg.grouped_id, edited_at=message_tg.edit_date.replace(tzinfo=timezone.utc) if message_tg.edit_date else None, is_pinned=message_tg.pinned or False)
                            temp_posts_buffer_for_db_add.append(new_post_db_obj); newly_added_post_objects_in_session.append(new_post_db_obj); collected_for_this_channel_this_run += 1; total_posts_collected += 1
                        if temp_posts_buffer_for_db_add: db_session.add_all(temp_posts_buffer_for_db_add); logger.info(f"  Добавлено в сессию {collected_for_this_channel_this_run} постов.")
                        if latest_post_id_seen_this_run > (channel_db_obj.last_processed_post_id or 0): channel_db_obj.last_processed_post_id = latest_post_id_seen_this_run; db_session.add(channel_db_obj); logger.info(f"  Обновлен last_id для канала: {latest_post_id_seen_this_run}")
                        if newly_added_post_objects_in_session:
                            logger.info(f"  Celery: Сбор комментариев для {len(newly_added_post_objects_in_session)} новых постов..."); await db_session.flush(); comments_collected_channel_total = 0
                            for post_obj_in_db in newly_added_post_objects_in_session:
                                current_post_comm_count = 0
                                try:
                                    async for comment_msg in tg_client.iter_messages(entity=channel_entity_tg, limit=settings.COMMENT_FETCH_LIMIT, reply_to=post_obj_in_db.telegram_post_id):
                                        comment_msg: Message
                                        if comment_msg.action or not (comment_msg.text or comment_msg.media or comment_msg.poll): continue
                                        if (await db_session.execute(select(Comment.id).where(Comment.telegram_comment_id == comment_msg.id, Comment.post_id == post_obj_in_db.id))).scalar_one_or_none() is not None: continue
                                        comm_text, comm_caption = (None, comment_msg.text) if comment_msg.media and comment_msg.text else (comment_msg.text, None)
                                        comm_media_type, comm_media_info = await _process_media_for_db(comment_msg.media); comm_reactions = await _process_reactions_for_db(comment_msg.reactions)
                                        comm_reply_to_id = comment_msg.reply_to.reply_to_msg_id if comment_msg.reply_to and hasattr(comment_msg.reply_to, 'reply_to_msg_id') else None
                                        comm_user_id, comm_user_username, comm_user_fullname = (comment_msg.from_id.user_id, None, None) # Это нужно будет исправить, чтобы получать username/fullname
                                        # --- НАЧАЛО: Получение данных об авторе комментария ---
                                        if isinstance(comment_msg.sender, TelethonUserType):
                                            comm_user_id = comment_msg.sender.id
                                            comm_user_username = comment_msg.sender.username
                                            comm_user_fullname = f"{comment_msg.sender.first_name or ''} {comment_msg.sender.last_name or ''}".strip() or None
                                        elif comment_msg.from_id and isinstance(comment_msg.from_id, PeerUser): # Редко, но бывает
                                            comm_user_id = comment_msg.from_id.user_id
                                            # Для username/fullname нужно будет сделать get_entity, если они важны и не в sender
                                        # --- КОНЕЦ: Получение данных об авторе комментария ---
                                        new_comm_obj = Comment(telegram_comment_id=comment_msg.id, post_id=post_obj_in_db.id, telegram_user_id=comm_user_id, user_username=comm_user_username, user_fullname=comm_user_fullname, text_content=comm_text or "", commented_at=comment_msg.date.replace(tzinfo=timezone.utc) if comment_msg.date else datetime.now(timezone.utc), reactions=comm_reactions, reply_to_telegram_comment_id=comm_reply_to_id, media_type=comm_media_type, media_content_info=comm_media_info, caption_text=comm_caption, edited_at=comment_msg.edit_date.replace(tzinfo=timezone.utc) if comment_msg.edit_date else None)
                                        db_session.add(new_comm_obj); current_post_comm_count +=1; total_comments_collected +=1
                                    if current_post_comm_count > 0: await db_session.execute(update(Post).where(Post.id == post_obj_in_db.id).values(comments_count=(Post.comments_count if Post.comments_count is not None else 0) + current_post_comm_count)); comments_collected_channel_total += current_post_comm_count
                                except TelethonMessageIdInvalidError: logger.warning(f"    Celery: Комментарии для поста {post_obj_in_db.telegram_post_id} не найдены (TMI).")
                                except FloodWaitError as fwe_comm: logger.warning(f"    !!! Celery: FloodWait комм. поста {post_obj_in_db.telegram_post_id}: {fwe_comm.seconds}s"); await asyncio.sleep(fwe_comm.seconds + 5)
                                except Exception as e_comm_loop: logger.error(f"    Celery: НЕОЖИДАННАЯ ошибка комм. поста {post_obj_in_db.telegram_post_id}: {type(e_comm_loop).__name__} - {e_comm_loop}")
                            if comments_collected_channel_total > 0 : logger.info(f"    Собрано {comments_collected_channel_total} комментариев для канала.")
                    except (ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError) as e_ch_access: logger.warning(f"  Канал {channel_db_obj.id} недоступен: {e_ch_access}. Деактивируем."); channel_db_obj.is_active = False; db_session.add(channel_db_obj)
                    except FloodWaitError as fwe_ch: logger.warning(f"  FloodWait для канала {channel_db_obj.title}: {fwe_ch.seconds} сек."); await asyncio.sleep(fwe_ch.seconds + 5)
                    except Exception as e_ch_proc: logger.error(f"  Celery: Ошибка обработки канала '{channel_db_obj.title}': {type(e_ch_proc).__name__} - {e_ch_proc}"); traceback.print_exc(limit=1)
                await db_session.commit() ; logger.info(f"\nCelery: Все изменения сохранены в БД. Обработано: {total_channels_processed} каналов.")
            return f"Celery: Сбор данных завершен успешно. Статистика: {total_channels_processed}ch, {total_posts_collected}p, {total_comments_collected}c."
        except ConnectionRefusedError as e_auth: logger.error(f"!!! Celery: ОШИБКА АВТОРИЗАЦИИ TELETHON: {e_auth}"); raise
        except Exception as e_async_logic: logger.error(f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_collector: {type(e_async_logic).__name__} {e_async_logic}"); traceback.print_exc(); raise
        finally:
            if tg_client and tg_client.is_connected(): logger.info("Celery: Отключение Telegram..."); await tg_client.disconnect()
            if local_async_engine: logger.info("Celery: Закрытие БД..."); await local_async_engine.dispose()
    try:
        # Используем asyncio.run() для Python 3.7+
        result_message = asyncio.run(_async_main_logic_collector())
        task_duration = time.time() - task_start_time; logger.info(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except ConnectionRefusedError as e_auth_final: 
        task_duration = time.time() - task_start_time
        logger.error(f"!!! Celery: ОШИБКА АВТОРИЗАЦИИ в таске '{self.name}' (за {task_duration:.2f} сек): {e_auth_final}. НЕ ПОВТОРЯТЬ.")
        # Не используем self.retry() для ошибок авторизации, так как они обычно требуют ручного вмешательства
        raise e_auth_final # Перевыбрасываем, чтобы Celery отметил задачу как FAILED
    except Exception as e_task_level: 
        task_duration = time.time() - task_start_time
        logger.error(f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}")
        traceback.print_exc()
        try: 
            # Убедимся, что default_retry_delay это число
            retry_delay_seconds = self.default_retry_delay if isinstance(self.default_retry_delay, (int, float)) else 300 # 5 минут по умолчанию, если не число
            countdown = int(retry_delay_seconds * (2 ** self.request.retries))
            logger.info(f"Celery: Retry ({self.request.retries + 1}/{self.max_retries}) таска {self.request.id} через {countdown} сек")
            raise self.retry(exc=e_task_level, countdown=countdown)
        except self.MaxRetriesExceededError: 
            logger.error(f"Celery: Max retries для таска {self.request.id}. Ошибка: {e_task_level}")
            raise e_task_level # Перевыбрасываем, чтобы Celery отметил задачу как FAILED
        except Exception as e_retry_logic: 
            logger.error(f"Celery: Ошибка в логике retry: {e_retry_logic}")
            raise e_task_level # Перевыбрасываем, чтобы Celery отметил задачу как FAILED

# --- ЗАДАЧА СУММАРИЗАЦИИ (AI) ---
# (остается без изменений, как в вашем коде)
@celery_instance.task(name="summarize_top_posts", bind=True, max_retries=2, default_retry_delay=300)
def summarize_top_posts_task(self, hours_ago=48, top_n=5):
    task_start_time = time.time(); logger.info(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (AI Суммаризация топ-{top_n} постов за {hours_ago}ч)...")
    if not settings.OPENAI_API_KEY: logger.error("Ошибка: OPENAI_API_KEY не настроен."); return "Config error OpenAI"
    try: openai_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    except Exception as e_init_openai: logger.error(f"Ошибка OpenAI init: {e_init_openai}"); raise self.retry(exc=e_init_openai)
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL_FOR_ALEMBIC.replace("postgresql://", "postgresql+asyncpg://") if settings.DATABASE_URL_FOR_ALEMBIC else settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
    async def _async_main_logic_summarizer():
        local_async_engine = None; processed_posts_count = 0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)
            async with LocalAsyncSessionFactory() as db_session:
                time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
                active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery()
                stmt_posts_to_summarize = (select(Post).join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id).where(Post.posted_at >= time_threshold, Post.summary_text == None).order_by(desc(Post.comments_count)).limit(top_n))
                posts_to_process = (await db_session.execute(stmt_posts_to_summarize)).scalars().all()
                if not posts_to_process: logger.info(f"  Не найдено постов для суммаризации."); return "Нет постов для суммаризации."
                logger.info(f"  Найдено {len(posts_to_process)} постов для суммаризации.")
                for post_obj in posts_to_process:
                    text_to_summarize = post_obj.caption_text if post_obj.caption_text else post_obj.text_content
                    if not text_to_summarize or len(text_to_summarize.strip()) < 30 : 
                        logger.info(f"    Пост ID {post_obj.id} слишком короткий или без текста/подписи, пропускаем."); continue
                    logger.info(f"    Суммаризация поста ID {post_obj.id}...")
                    try:
                        summary_prompt = f"Текст поста:\n---\n{text_to_summarize[:4000]}\n---\nНапиши краткое резюме (1-3 предложения на русском) основной мысли этого поста."
                        completion = await asyncio.to_thread(openai_client.chat.completions.create, model="gpt-3.5-turbo", messages=[{"role": "system", "content": "Ты AI-ассистент, генерирующий краткие резюме текстов."}, {"role": "user", "content": summary_prompt}], temperature=0.3, max_tokens=200)
                        summary = completion.choices[0].message.content.strip()
                        if summary: post_obj.summary_text = summary; post_obj.updated_at = datetime.now(timezone.utc); db_session.add(post_obj); processed_posts_count += 1; logger.info(f"      Резюме для поста ID {post_obj.id} получено.")
                    except OpenAIError as e_llm: logger.error(f"    !!! Ошибка OpenAI API поста ID {post_obj.id}: {e_llm}")
                    except Exception as e_sum: logger.error(f"    !!! Неожиданная ошибка суммаризации поста ID {post_obj.id}: {e_sum}"); traceback.print_exc(limit=1)
                if processed_posts_count > 0: await db_session.commit(); logger.info(f"  Успешно суммаризировано {processed_posts_count} постов.")
            return f"Суммаризация завершена. Обработано: {processed_posts_count} постов."
        except Exception as e_async_sum: logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_summarizer: {e_async_sum}"); traceback.print_exc(); raise
        finally:
            if local_async_engine: await local_async_engine.dispose()
    try:
        result_message = asyncio.run(_async_main_logic_summarizer())
        task_duration = time.time() - task_start_time; logger.info(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except Exception as e_task_sum: task_duration = time.time() - task_start_time; logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {e_task_sum}"); traceback.print_exc(); raise self.retry(exc=e_task_sum)

# --- ОБНОВЛЕННАЯ ЗАДАЧА ОТПРАВКИ ДАЙДЖЕСТА ---
# (остается без изменений, как в вашем коде)
@celery_instance.task(name="send_daily_digest", bind=True, max_retries=3, default_retry_delay=180)
def send_daily_digest_task(self, hours_ago_posts=24, top_n_metrics=3):
    task_start_time = time.time()
    logger.info(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Отправка РАСШИРЕННОГО ежедневного дайджеста)...")

    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_TARGET_CHAT_ID:
        error_msg = "Ошибка: TELEGRAM_BOT_TOKEN или TELEGRAM_TARGET_CHAT_ID не настроены."
        logger.error(error_msg); return error_msg

    async def _async_send_digest_logic():
        bot = telegram.Bot(token=settings.TELEGRAM_BOT_TOKEN)
        ASYNC_DB_URL_FOR_TASK_DIGEST = settings.DATABASE_URL_FOR_ALEMBIC.replace("postgresql://", "postgresql+asyncpg://") if settings.DATABASE_URL_FOR_ALEMBIC else settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
        local_async_engine_digest = None
        message_parts = [] 
        
        try:
            local_async_engine_digest = create_async_engine(ASYNC_DB_URL_FOR_TASK_DIGEST, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactoryDigest = sessionmaker(bind=local_async_engine_digest, class_=AsyncSession, expire_on_commit=False)

            async with LocalAsyncSessionFactoryDigest() as db_session:
                time_threshold_posts = datetime.now(timezone.utc) - timedelta(hours=hours_ago_posts)
                message_parts.append(helpers.escape_markdown(f" digest for Insight-Compass за последние {hours_ago_posts} часа:\n", version=2))
                
                active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery("active_channels_sq_digest")
                stmt_new_posts_count = (select(func.count(Post.id))
                                        .join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id)
                                        .where(Post.posted_at >= time_threshold_posts))
                new_posts_count = (await db_session.execute(stmt_new_posts_count)).scalar_one_or_none() or 0
                message_parts.append(helpers.escape_markdown(f"📰 Всего новых постов (из активных каналов): {new_posts_count}\n", version=2))

                async def get_top_posts_by_metric(metric_column_or_expr_name: str, metric_display_name: str, is_sum_from_jsonb_flag=False):
                    top_posts_list = []
                    p_alias_name = f"p_digest_{''.join(filter(str.isalnum, metric_display_name.lower()))}"
                    p_alias = aliased(Post, name=p_alias_name)
                    select_fields = [p_alias.link, p_alias.summary_text, p_alias.post_sentiment_label, Channel.title.label("channel_title")]
                    stmt_top = select(*select_fields)
                    order_by_col = None
                    if is_sum_from_jsonb_flag:
                        rx_elements_cte_name = f"rx_elements_cte_{''.join(filter(str.isalnum, metric_display_name.lower()))}"
                        rx_elements_cte = (select(p_alias.id.label("post_id_for_reactions"), cast(func.jsonb_array_elements(p_alias.reactions).op('->>')('count'), SAInteger).label("reaction_item_count")).select_from(p_alias).where(p_alias.reactions.isnot(None)).where(func.jsonb_typeof(p_alias.reactions) == 'array').cte(rx_elements_cte_name))
                        sum_rx_cte_name = f"sum_rx_cte_{''.join(filter(str.isalnum, metric_display_name.lower()))}"
                        sum_reactions_cte = (select(rx_elements_cte.c.post_id_for_reactions.label("post_id"), func.sum(rx_elements_cte.c.reaction_item_count).label("metric_value_for_digest")).group_by(rx_elements_cte.c.post_id_for_reactions).cte(sum_rx_cte_name))
                        stmt_top = stmt_top.add_columns(sum_reactions_cte.c.metric_value_for_digest).outerjoin(sum_reactions_cte, p_alias.id == sum_reactions_cte.c.post_id)
                        order_by_col = sum_reactions_cte.c.metric_value_for_digest
                    else:
                        actual_metric_column = getattr(p_alias, metric_column_or_expr_name)
                        stmt_top = stmt_top.add_columns(actual_metric_column.label("metric_value_for_digest"))
                        order_by_col = actual_metric_column
                    stmt_top = stmt_top.join(Channel, p_alias.channel_id == Channel.id).where(Channel.is_active == True).where(p_alias.posted_at >= time_threshold_posts).where(p_alias.summary_text.isnot(None)).order_by(order_by_col.desc().nullslast()).limit(top_n_metrics)
                    for row in (await db_session.execute(stmt_top)).all():
                        top_posts_list.append({"link": row.link, "summary": row.summary_text, "sentiment": row.post_sentiment_label, "channel_title": row.channel_title, "metric_value": row.metric_value_for_digest})
                    return top_posts_list

                tops_to_include = [
                    {"metric_name": "comments_count", "label": "💬 Топ по комментариям", "emoji": "🗣️", "unit": "Комм."},
                    {"metric_name": "views_count", "label": "👀 Топ по просмотрам", "emoji": "👁️", "unit": "Просмотров"},
                    {"metric_name": "reactions", "label": "❤️ Топ по сумме реакций", "emoji": "👍", "unit": "Реакций", "is_sum_jsonb": True},
                ]
                for top_conf in tops_to_include:
                    posts_data = await get_top_posts_by_metric(top_conf["metric_name"], top_conf["label"], is_sum_from_jsonb_flag=top_conf.get("is_sum_jsonb", False))
                    if posts_data:
                        message_parts.append(f"\n{helpers.escape_markdown(top_conf['label'], version=2)} \\(с AI\\-резюме, топ\\-{len(posts_data)}\\):\n")
                        for i, p_data in enumerate(posts_data):
                            link_md = helpers.escape_markdown(p_data["link"] or "#", version=2)
                            summary_md = helpers.escape_markdown(p_data["summary"] or "Резюме отсутствует.", version=2)
                            channel_md = helpers.escape_markdown(p_data["channel_title"] or "Неизв. канал", version=2)
                            metric_val_md = helpers.escape_markdown(str(p_data["metric_value"] or 0), version=2)
                            s_emoji = "😐 "; s_label_text = ""
                            if p_data["sentiment"]:
                                s_label_text = helpers.escape_markdown(p_data["sentiment"].capitalize(), version=2)
                                if p_data["sentiment"] == "positive": s_emoji = "😊 "
                                elif p_data["sentiment"] == "negative": s_emoji = "😠 "
                            else: s_label_text = helpers.escape_markdown("N/A", version=2)
                            message_parts.append(f"\n{i+1}\\. {channel_md} [{helpers.escape_markdown('Пост',version=2)}]({link_md})\n   {top_conf['emoji']} {helpers.escape_markdown(top_conf['unit'], version=2)}: {metric_val_md} {s_emoji}{s_label_text}\n   📝 _{summary_md}_\n")
            digest_message_final = "".join(message_parts)
            if len(digest_message_final) > 4096: digest_message_final = digest_message_final[:4090] + helpers.escape_markdown("...", version=2); logger.warning("  ВНИМАНИЕ: Дайджест был обрезан из-за превышения лимита Telegram.")
            logger.info(f"  Финальное сообщение дайджеста для Telegram:\n---\n{digest_message_final[:500]}...\n---") # Логгируем только начало
            await bot.send_message(chat_id=settings.TELEGRAM_TARGET_CHAT_ID, text=digest_message_final, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
            return f"Расширенный дайджест успешно отправлен."
        except Exception as e_digest_logic: logger.error(f"!!! Ошибка в _async_send_digest_logic: {e_digest_logic}"); traceback.print_exc(); raise
        finally:
            if local_async_engine_digest: await local_async_engine_digest.dispose()
    try:
        result_message = asyncio.run(_async_send_digest_logic())
        task_duration = time.time() - task_start_time; logger.info(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except Exception as e_task_digest: task_duration = time.time() - task_start_time; logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {e_task_digest}"); traceback.print_exc(); raise self.retry(exc=e_task_digest)

# --- ЗАДАЧА АНАЛИЗА ТОНАЛЬНОСТИ (AI) ---
# (остается без изменений, как в вашем коде)
@celery_instance.task(name="analyze_posts_sentiment", bind=True, max_retries=2, default_retry_delay=300)
def analyze_posts_sentiment_task(self, limit_posts_to_analyze=10):
    task_start_time = time.time(); logger.info(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Анализ тональности, лимит: {limit_posts_to_analyze})...")
    if not settings.OPENAI_API_KEY: logger.error("Ошибка: OPENAI_API_KEY не настроен."); return "Config error OpenAI"
    try: openai_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    except Exception as e_init_openai_sentiment: logger.error(f"Ошибка OpenAI init: {e_init_openai_sentiment}"); raise self.retry(exc=e_init_openai_sentiment)
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL_FOR_ALEMBIC.replace("postgresql://", "postgresql+asyncpg://") if settings.DATABASE_URL_FOR_ALEMBIC else settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
    async def _async_main_logic_sentiment_analyzer():
        local_async_engine = None; analyzed_posts_count = 0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)
            async with LocalAsyncSessionFactory() as db_session:
                active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery()
                stmt_posts_to_analyze = (select(Post).join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id)
                                         .where(or_(Post.text_content.isnot(None), Post.caption_text.isnot(None))) 
                                         .where(Post.post_sentiment_label == None)
                                         .order_by(Post.posted_at.asc()).limit(limit_posts_to_analyze)) # asc, чтобы старые сначала
                posts_to_process = (await db_session.execute(stmt_posts_to_analyze)).scalars().all()
                if not posts_to_process: logger.info(f"  Не найдено постов для анализа тональности."); return "Нет постов для анализа."
                logger.info(f"  Найдено {len(posts_to_process)} постов для анализа.")
                for post_obj in posts_to_process:
                    text_for_analysis = post_obj.caption_text if post_obj.caption_text else post_obj.text_content
                    if not text_for_analysis or not text_for_analysis.strip(): 
                        logger.info(f"    Пост ID {post_obj.id} не имеет текста/подписи для анализа, пропускаем."); continue
                    logger.info(f"    Анализ тональности поста ID {post_obj.id}...")
                    s_label, s_score = "neutral", 0.0 
                    try: 
                        prompt = f"Определи тональность текста (JSON: sentiment_label: [positive,negative,neutral,mixed], sentiment_score: [-1.0,1.0]):\n---\n{text_for_analysis[:3500]}\n---\nJSON_RESPONSE:"
                        completion = await asyncio.to_thread(openai_client.chat.completions.create, model="gpt-3.5-turbo", messages=[{"role": "system", "content": "AI-аналитик тональности (JSON)."}, {"role": "user", "content": prompt}], temperature=0.2, max_tokens=60, response_format={"type": "json_object"})
                        raw_resp = completion.choices[0].message.content
                        if raw_resp:
                            try: 
                                data = json.loads(raw_resp)
                                s_label = data.get("sentiment_label", "neutral")
                                s_score = float(data.get("sentiment_score", 0.0))
                                if s_label not in ["positive","negative","neutral","mixed"]: logger.warning(f"      ПРЕДУПРЕЖДЕНИЕ: LLM вернул невалидный sentiment_label '{s_label}'. Установлен 'neutral'."); s_label="neutral"
                                if not (-1.0 <= s_score <= 1.0): logger.warning(f"      ПРЕДУПРЕЖДЕНИЕ: LLM вернул sentiment_score вне диапазона: {s_score}. Установлен 0.0."); s_score=0.0
                            except (json.JSONDecodeError, TypeError, ValueError) as e_json: logger.error(f"  Ошибка парсинга JSON от LLM ({e_json}): {raw_resp}")
                        else: logger.warning(f"      OpenAI вернул пустой ответ для поста ID {post_obj.id}.")
                    except OpenAIError as e_llm_sentiment: logger.error(f"    !!! Ошибка OpenAI API для поста ID {post_obj.id}: {e_llm_sentiment}"); continue 
                    except Exception as e_sa_general: logger.error(f"    !!! Неожиданная ошибка анализа поста ID {post_obj.id}: {e_sa_general}"); traceback.print_exc(limit=1); continue 
                    post_obj.post_sentiment_label = s_label; post_obj.post_sentiment_score = s_score; post_obj.updated_at = datetime.now(timezone.utc)
                    db_session.add(post_obj); analyzed_posts_count += 1
                    logger.info(f"      Тональность поста ID {post_obj.id}: {s_label} ({s_score:.2f})")
                if analyzed_posts_count > 0: await db_session.commit(); logger.info(f"  Проанализировано {analyzed_posts_count} постов.")
            return f"Анализ тональности завершен. Обработано: {analyzed_posts_count} постов."
        except Exception as e_async_sa_main: logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_sentiment_analyzer: {e_async_sa_main}"); traceback.print_exc(); raise
        finally:
            if local_async_engine: await local_async_engine.dispose()
    try:
        result_message = asyncio.run(_async_main_logic_sentiment_analyzer())
        task_duration = time.time() - task_start_time; logger.info(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except Exception as e_task_sa_main: task_duration = time.time() - task_start_time; logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {e_task_sa_main}"); traceback.print_exc(); raise self.retry(exc=e_task_sa_main)


# --- НАЧАЛО: НОВЫЕ ЗАДАЧИ ДЛЯ AI-АНАЛИЗА КОММЕНТАРИЕВ ---

@celery_instance.task(name="tasks.analyze_single_comment_ai_features", bind=True, max_retries=2, default_retry_delay=60 * 2) # Таймаут для одной попытки
def analyze_single_comment_ai_features_task(self, comment_id: int):
    """
    Анализирует один комментарий с помощью LLM для извлечения тем, проблем, вопросов, предложений.
    """
    task_start_time = time.time()
    logger.info(f"[AICommentAnalysis] Запущен анализ для comment_id: {comment_id} (Task ID: {self.request.id})")

    if not settings.OPENAI_API_KEY:
        logger.error("[AICommentAnalysis] Ошибка: OPENAI_API_KEY не настроен.")
        return f"Config error OpenAI for comment_id: {comment_id}"
    
    # Используем тот же подход к БД, что и в других ваших задачах
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL_FOR_ALEMBIC.replace("postgresql://", "postgresql+asyncpg://") if settings.DATABASE_URL_FOR_ALEMBIC else settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_analyze_comment_logic():
        local_async_engine = None
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)

            async with LocalAsyncSessionFactory() as db_session:
                comment_stmt = select(Comment).where(Comment.id == comment_id)
                comment_result = await db_session.execute(comment_stmt)
                comment = comment_result.scalar_one_or_none()

                if not comment:
                    logger.warning(f"[AICommentAnalysis] Комментарий с ID {comment_id} не найден.")
                    return f"Comment ID {comment_id} not found."

                # Собираем текст для анализа (основной текст + подпись, если есть)
                text_to_analyze = comment.text_content or ""
                if comment.caption_text:
                    text_to_analyze = f"{text_to_analyze}\n[Подпись к медиа]: {comment.caption_text}".strip()

                if not text_to_analyze:
                    logger.info(f"[AICommentAnalysis] Комментарий ID {comment_id} не содержит текста для анализа. Помечаем как обработанный.")
                    update_stmt_empty = update(Comment).where(Comment.id == comment_id).values(
                        ai_analysis_completed_at=datetime.now(timezone.utc),
                        extracted_topics=[], extracted_problems=[], extracted_questions=[], extracted_suggestions=[]
                    )
                    await db_session.execute(update_stmt_empty)
                    await db_session.commit()
                    return f"Comment ID {comment_id} has no text to analyze."

                prompt_template = """Ты — продвинутый AI-аналитик, специализирующийся на анализе пользовательских комментариев из Telegram. Твоя задача — внимательно прочитать следующий комментарий и извлечь из него указанную информацию.

Комментарий:
"{comment_text}"

Проанализируй комментарий и верни результат в формате JSON со следующими ключами:
- "topics": список из 1-5 ключевых тем или сущностей, обсуждаемых в комментарии (например, ["цена", "доставка", "обновление приложения"]). Если тем нет, верни пустой список.
- "problems": список из 1-3 основных проблем или болей, высказанных пользователем (например, ["товар пришел сломанный", "долго ждать ответа поддержки"]). Если проблем нет, верни пустой список.
- "questions": список из 1-3 конкретных вопросов, заданных пользователем (например, ["когда починят баг?", "можно ли вернуть деньги?"]). Если вопросов нет, верни пустой список.
- "suggestions": список из 1-3 предложений или идей, высказанных пользователем (например, ["добавьте темную тему", "сделайте инструкцию понятнее"]). Если предложений нет, верни пустой список.

Важно:
- Извлекай только то, что явно или с высокой вероятностью присутствует в тексте. Не додумывай.
- Списки должны содержать короткие, емкие фразы или ключевые слова на русском языке.
- Если какой-то категории нет в комментарии, соответствующий ключ в JSON должен иметь значение пустого списка `[]`.
- Весь ответ должен быть одним JSON-объектом. Строго JSON. Не добавляй никаких пояснений вне JSON.
"""
                prompt = prompt_template.format(comment_text=text_to_analyze[:settings.LLM_MAX_PROMPT_LENGTH or 3800]) # Ограничиваем длину промта

                llm_response_str = await одиночный_запрос_к_llm(
                    prompt, 
                    модель=settings.OPENAI_DEFAULT_MODEL_FOR_TASKS or "gpt-3.5-turbo", # Можно использовать отдельную модель для задач
                    температура=0.2, 
                    макс_токены=300 # JSON обычно не очень большой
                )

                if not llm_response_str:
                    logger.error(f"[AICommentAnalysis] Нет ответа от LLM для комментария ID {comment_id}.")
                    # Можно не помечать как обработанный, чтобы повторить позже, или пометить с ошибкой
                    return f"No LLM response for comment ID {comment_id}."
                
                try:
                    # Попытка извлечь JSON из ответа LLM
                    if llm_response_str.strip().startswith("```json"):
                        llm_response_str = llm_response_str.split("```json", 1)[1].rsplit("```", 1)[0].strip()
                    elif llm_response_str.strip().startswith("```"): # Общий случай для ```
                         llm_response_str = llm_response_str.split("```",1)[1].rsplit("```",1)[0].strip()


                    analysis_data = json.loads(llm_response_str)
                    
                    # Валидация структуры ответа (проверяем, что это dict и есть нужные ключи типа list)
                    required_keys = {"topics": list, "problems": list, "questions": list, "suggestions": list}
                    valid_structure = isinstance(analysis_data, dict) and \
                                      all(key in analysis_data and isinstance(analysis_data[key], req_type) 
                                          for key, req_type in required_keys.items())

                    if not valid_structure:
                        logger.error(f"[AICommentAnalysis] Некорректная структура JSON от LLM для comment_id {comment_id}. Ответ: {llm_response_str}")
                        # Можно не обновлять или обновить с пустыми полями и ошибкой
                        return f"Invalid JSON structure from LLM for comment ID {comment_id}."

                    update_values = {
                        "extracted_topics": analysis_data.get("topics", []),
                        "extracted_problems": analysis_data.get("problems", []),
                        "extracted_questions": analysis_data.get("questions", []),
                        "extracted_suggestions": analysis_data.get("suggestions", []),
                        "ai_analysis_completed_at": datetime.now(timezone.utc)
                    }
                    
                    update_stmt = update(Comment).where(Comment.id == comment_id).values(**update_values)
                    await db_session.execute(update_stmt)
                    await db_session.commit()
                    logger.info(f"[AICommentAnalysis] Успешно проанализирован и обновлен комментарий ID {comment_id}.")
                    return f"Successfully analyzed comment ID {comment_id}."

                except json.JSONDecodeError:
                    logger.error(f"[AICommentAnalysis] Ошибка декодирования JSON от LLM для comment_id {comment_id}. Ответ: {llm_response_str}")
                    return f"JSONDecodeError for comment ID {comment_id}."
                except Exception as e_proc:
                    logger.error(f"[AICommentAnalysis] Ошибка обработки ответа LLM для comment_id {comment_id}: {e_proc}", exc_info=True)
                    return f"Error processing LLM response for comment ID {comment_id}."

        except Exception as e_general:
            logger.error(f"[AICommentAnalysis] Общая ошибка для comment_id {comment_id}: {e_general}", exc_info=True)
            raise # Перевыбрасываем, чтобы Celery мог сделать retry
        finally:
            if local_async_engine:
                await local_async_engine.dispose()

    try:
        result_message = asyncio.run(_async_analyze_comment_logic())
        task_duration = time.time() - task_start_time
        logger.info(f"Celery таск '{self.name}' (comment_id: {comment_id}) завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_level:
        task_duration = time.time() - task_start_time
        logger.error(f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (comment_id: {comment_id}) (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}")
        traceback.print_exc()
        try:
            retry_delay_seconds = self.default_retry_delay if isinstance(self.default_retry_delay, (int, float)) else 120
            countdown = int(retry_delay_seconds * (2 ** self.request.retries))
            logger.info(f"Celery: Retry ({self.request.retries + 1}/{self.max_retries}) таска {self.request.id} (comment_id: {comment_id}) через {countdown} сек")
            raise self.retry(exc=e_task_level, countdown=countdown)
        except self.MaxRetriesExceededError:
            logger.error(f"Celery: Max retries для таска {self.request.id} (comment_id: {comment_id}). Ошибка: {e_task_level}")
            # Можно пометить комментарий как "не удалось обработать" в БД, если нужно
            raise e_task_level
        except Exception as e_retry_logic:
            logger.error(f"Celery: Ошибка в логике retry для таска {self.request.id} (comment_id: {comment_id}): {e_retry_logic}")
            raise e_task_level


@celery_instance.task(name="tasks.enqueue_comments_for_ai_feature_analysis", bind=True)
def enqueue_comments_for_ai_feature_analysis_task(self, limit_comments_to_queue: int = 100, older_than_hours: Optional[int] = None, channel_id_filter: Optional[int] = None):
    """
    Выбирает комментарии для AI-анализа (извлечение фич) и ставит их в очередь.
    """
    task_start_time = time.time()
    logger.info(f"[AICommentQueue] Запуск задачи постановки комментариев в очередь для AI-анализа. Лимит: {limit_comments_to_queue}, Старше часов: {older_than_hours}, Канал ID: {channel_id_filter} (Task ID: {self.request.id})")
    
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL_FOR_ALEMBIC.replace("postgresql://", "postgresql+asyncpg://") if settings.DATABASE_URL_FOR_ALEMBIC else settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

    async def _async_enqueue_logic():
        local_async_engine = None
        enqueued_count = 0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)

            async with LocalAsyncSessionFactory() as db_session:
                stmt = select(Comment.id).where(Comment.text_content.isnot(None)).where(Comment.text_content != "")
                
                if older_than_hours is not None:
                    time_threshold = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
                    stmt = stmt.where(
                        (Comment.ai_analysis_completed_at == None) | 
                        (Comment.ai_analysis_completed_at < time_threshold)
                    )
                else:
                    stmt = stmt.where(Comment.ai_analysis_completed_at == None) # Только те, что еще не анализировались
                
                if channel_id_filter:
                    # Присоединяем Post и Channel для фильтрации по channel_id
                    stmt = stmt.join(Post, Comment.post_id == Post.id).where(Post.channel_id == channel_id_filter)

                # Берем сначала более старые не обработанные комментарии
                stmt = stmt.order_by(Comment.commented_at.asc()).limit(limit_comments_to_queue) 
                
                comment_ids_result = await db_session.execute(stmt)
                comment_ids = comment_ids_result.scalars().all()

                if not comment_ids:
                    logger.info("[AICommentQueue] Не найдено комментариев для постановки в очередь.")
                    return "No comments found to queue."

                logger.info(f"[AICommentQueue] Найдено {len(comment_ids)} комментариев для постановки в очередь. Запускаю задачи...")
                for comment_id in comment_ids:
                    analyze_single_comment_ai_features_task.delay(comment_id)
                    enqueued_count += 1
                
                return f"Successfully enqueued {enqueued_count} comments for AI feature analysis."

        except Exception as e:
            logger.error(f"[AICommentQueue] Ошибка: {e}", exc_info=True)
            raise # Перевыбрасываем для обработки Celery
        finally:
            if local_async_engine:
                await local_async_engine.dispose()
    
    try:
        result_message = asyncio.run(_async_enqueue_logic())
        task_duration = time.time() - task_start_time
        logger.info(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_level:
        task_duration = time.time() - task_start_time
        logger.error(f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}")
        traceback.print_exc()
        # Для этой задачи можно не делать retry, так как она только ставит в очередь
        raise e_task_level


# --- КОНЕЦ: НОВЫЕ ЗАДАЧИ ДЛЯ AI-АНАЛИЗА КОММЕНТАРИЕВ ---