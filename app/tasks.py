# app/tasks.py

import asyncio
import os
import time
import traceback
import json
from datetime import timezone, datetime, timedelta, date
from typing import List, Dict, Any, Optional, Tuple, AsyncGenerator
import logging

import openai
from openai import OpenAIError 

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.future import select
from sqlalchemy import desc, func, update, cast, literal_column, nullslast, Integer as SAInteger, or_
from sqlalchemy.orm import aliased
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import text # Добавлено для SQL запроса

import telegram
from telegram.constants import ParseMode
from telegram import helpers

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
from telethon.requestiter import RequestIter

from app.celery_app import celery_instance
from app.core.config import settings
from app.models.telegram_data import Channel, Post, Comment 
from app.db.session import get_async_session_context_manager 
from app.schemas.ui_schemas import PostRefreshMode, CommentRefreshMode 

try:
    from app.services.llm_service import одиночный_запрос_к_llm
except ImportError:
    async def одиночный_запрос_к_llm(prompt: str, модель: str, is_json_response_expected: bool = False, **kwargs) -> Optional[str]:
        current_logger = logging.getLogger(__name__)
        prompt_preview = prompt[:100].replace('\n', ' ')
        current_logger.warning(f"ЗАГЛУШКА LLM: Модель='{модель}', JSON ожидается={is_json_response_expected}. Промпт (начало): '{prompt_preview}...'")

        if is_json_response_expected:
            if "Определи тональность текста (JSON" in prompt:
                return json.dumps({"sentiment_label": "neutral-mock", "sentiment_score": 0.0})
            elif "Проанализируй комментарий и верни результат в формате JSON со следующими ключами" in prompt:
                return json.dumps({"topics": ["mock-topic"], "problems": [], "questions": [], "suggestions": ["mock-suggestion"]})
            else:
                return json.dumps({"message": "Заглушка LLM: JSON ответ", "data": {}})
        return f"Заглушка LLM: Модель '{модель}' получила промпт (не JSON)."

logger = celery_instance.log.get_default_logger()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ СБОРА ДАННЫХ ---
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


async def _helper_fetch_and_process_comments_for_post(
    tg_client: TelegramClient,
    db: AsyncSession,
    post_db_obj: Post, 
    tg_channel_entity: TelethonChannelType,
    comment_limit: int,
    log_prefix: str = "[CommentHelper]"
) -> Tuple[int, List[int]]: 
    new_comments_count_for_post = 0
    new_comment_ids_for_post: List[int] = []
    existing_comment_tg_ids_stmt = select(Comment.telegram_comment_id).where(Comment.post_id == post_db_obj.id)
    existing_comment_tg_ids_res = await db.execute(existing_comment_tg_ids_stmt)
    existing_comment_tg_ids_set = set(existing_comment_tg_ids_res.scalars().all())
    
    logger.debug(f"{log_prefix}    Пост ID {post_db_obj.id} (TG ID: {post_db_obj.telegram_post_id}): в БД уже есть {len(existing_comment_tg_ids_set)} комментариев. Запрашиваем из Telegram (лимит: {comment_limit}).")

    flood_wait_attempts_for_post = 0
    max_flood_wait_attempts = 2 

    while flood_wait_attempts_for_post < max_flood_wait_attempts:
        try:
            message_iterator: RequestIter = tg_client.iter_messages(
                entity=tg_channel_entity,
                limit=comment_limit, 
                reply_to=post_db_obj.telegram_post_id
            )
            async for tg_comment_msg in message_iterator:
                tg_comment_msg: Message
                if tg_comment_msg.action or not (tg_comment_msg.text or tg_comment_msg.media or tg_comment_msg.poll):
                    continue
                if tg_comment_msg.id in existing_comment_tg_ids_set: 
                    continue

                comm_text, comm_caption = (None, tg_comment_msg.text) if tg_comment_msg.media and tg_comment_msg.text else (tg_comment_msg.text, None)
                comm_media_type, comm_media_info = await _process_media_for_db(tg_comment_msg.media)
                comm_reactions = await _process_reactions_for_db(tg_comment_msg.reactions)
                comm_reply_to_id = tg_comment_msg.reply_to.reply_to_msg_id if tg_comment_msg.reply_to and hasattr(tg_comment_msg.reply_to, 'reply_to_msg_id') else None
                comm_user_id, comm_user_username, comm_user_fullname = (None, None, None)
                if isinstance(tg_comment_msg.sender, TelethonUserType):
                    comm_user_id = tg_comment_msg.sender.id; comm_user_username = tg_comment_msg.sender.username
                    comm_user_fullname = f"{tg_comment_msg.sender.first_name or ''} {tg_comment_msg.sender.last_name or ''}".strip() or None
                elif tg_comment_msg.from_id and isinstance(tg_comment_msg.from_id, PeerUser):
                    comm_user_id = tg_comment_msg.from_id.user_id

                new_comment_db = Comment(
                    telegram_comment_id=tg_comment_msg.id, post_id=post_db_obj.id,
                    telegram_user_id=comm_user_id, user_username=comm_user_username, user_fullname=comm_user_fullname,
                    text_content=comm_text or (comm_caption if not comm_text else ""),
                    commented_at=tg_comment_msg.date.replace(tzinfo=timezone.utc) if tg_comment_msg.date else datetime.now(timezone.utc),
                    reactions=comm_reactions, reply_to_telegram_comment_id=comm_reply_to_id,
                    media_type=comm_media_type, media_content_info=comm_media_info,
                    caption_text=comm_caption,
                    edited_at=tg_comment_msg.edit_date.replace(tzinfo=timezone.utc) if tg_comment_msg.edit_date else None,
                )
                db.add(new_comment_db)
                await db.flush() 
                if new_comment_db.id: new_comment_ids_for_post.append(new_comment_db.id)
                new_comments_count_for_post += 1
                existing_comment_tg_ids_set.add(tg_comment_msg.id) 

            if new_comments_count_for_post > 0:
                logger.info(f"{log_prefix}    Для поста ID {post_db_obj.id} (TG ID: {post_db_obj.telegram_post_id}) добавлено {new_comments_count_for_post} новых комментариев в БД.")
            
            break 

        except TelethonMessageIdInvalidError:
            logger.warning(f"{log_prefix}    Комментарии для поста {post_db_obj.telegram_post_id} (DB ID: {post_db_obj.id}) не найдены или недоступны в Telegram (MsgIdInvalid). Ранее собранных в БД: {len(existing_comment_tg_ids_set)}.")
            break
        except FloodWaitError as fwe_c:
            flood_wait_attempts_for_post += 1
            if flood_wait_attempts_for_post >= max_flood_wait_attempts:
                logger.error(f"{log_prefix}    Превышен лимит ({max_flood_wait_attempts}) попыток FloodWait для поста {post_db_obj.telegram_post_id}. Пропускаем сбор комм. для этого поста окончательно.")
                break
            else:
                logger.warning(f"{log_prefix}    FloodWait ({fwe_c.seconds}s) при сборе комм. для поста {post_db_obj.telegram_post_id} (попытка {flood_wait_attempts_for_post}/{max_flood_wait_attempts}). Ждем и пытаемся снова для этого же поста.")
                await asyncio.sleep(fwe_c.seconds + 5) 
        except Exception as e_c:
            logger.error(f"{log_prefix}    Ошибка сбора комм. для поста {post_db_obj.telegram_post_id} (DB ID: {post_db_obj.id}): {type(e_c).__name__} - {e_c}", exc_info=True)
            break
            
    return new_comments_count_for_post, new_comment_ids_for_post


async def _helper_fetch_and_process_posts_for_channel(
    tg_client: TelegramClient,
    db: AsyncSession,
    channel_db: Channel,
    iter_params: Dict[str, Any],
    update_existing_info_flag: bool = False,
    log_prefix: str = "[PostHelper]"
) -> Tuple[List[Post], List[Post], int, int, int]:
    posts_for_comment_scan_candidates: List[Post] = []
    newly_created_post_objects: List[Post] = []
    new_posts_count_channel = 0
    updated_posts_count_channel = 0
    latest_post_id_tg_seen_this_run = channel_db.last_processed_post_id or 0

    message_iterator: RequestIter = tg_client.iter_messages(**iter_params)
    async for tg_message in message_iterator:
        tg_message: Message
        if isinstance(tg_message, MessageService) or tg_message.action: continue
        if not (tg_message.text or tg_message.media or tg_message.poll): continue
        
        if tg_message.id > latest_post_id_tg_seen_this_run:
            latest_post_id_tg_seen_this_run = tg_message.id
            
        existing_post_stmt = select(Post).where(Post.telegram_post_id == tg_message.id, Post.channel_id == channel_db.id)
        existing_post_db = (await db.execute(existing_post_stmt)).scalar_one_or_none()
        
        post_text_content, post_caption_text = (None, tg_message.text) if tg_message.media and tg_message.text else (tg_message.text if not tg_message.media else None, None)
        media_type, media_info = await _process_media_for_db(tg_message.media)
        reactions_data = await _process_reactions_for_db(tg_message.reactions)
        reply_to_id = tg_message.reply_to.reply_to_msg_id if tg_message.reply_to and hasattr(tg_message.reply_to, 'reply_to_msg_id') else None
        sender_id_val = tg_message.from_id.user_id if isinstance(tg_message.from_id, PeerUser) else None
        link_val = f"https://t.me/{channel_db.username or f'c/{channel_db.id}'}/{tg_message.id}"
        posted_at_val = tg_message.date.replace(tzinfo=timezone.utc) if tg_message.date else datetime.now(timezone.utc)
        edited_at_val = tg_message.edit_date.replace(tzinfo=timezone.utc) if tg_message.edit_date else None
        
        api_comments_count = tg_message.replies.replies if tg_message.replies and tg_message.replies.replies is not None else 0

        post_for_comments_scan_candidate = None
        if not existing_post_db:
            new_post = Post(
                telegram_post_id=tg_message.id, channel_id=channel_db.id, link=link_val,
                text_content=post_text_content, caption_text=post_caption_text,
                views_count=tg_message.views, comments_count=api_comments_count, 
                posted_at=posted_at_val, reactions=reactions_data, media_type=media_type,
                media_content_info=media_info, reply_to_telegram_post_id=reply_to_id,
                forwards_count=tg_message.forwards, author_signature=tg_message.post_author,
                sender_user_id=sender_id_val, grouped_id=tg_message.grouped_id,
                edited_at=edited_at_val, is_pinned=tg_message.pinned or False
            )
            db.add(new_post); await db.flush(); post_for_comments_scan_candidate = new_post; new_posts_count_channel +=1
            newly_created_post_objects.append(new_post)
        elif update_existing_info_flag:
            existing_post_db.views_count = tg_message.views
            existing_post_db.reactions = reactions_data
            existing_post_db.forwards_count = tg_message.forwards
            existing_post_db.edited_at = edited_at_val
            existing_post_db.comments_count = api_comments_count 
            existing_post_db.text_content = post_text_content
            existing_post_db.caption_text = post_caption_text
            existing_post_db.media_type = media_type
            existing_post_db.media_content_info = media_info
            existing_post_db.is_pinned = tg_message.pinned or False
            existing_post_db.author_signature = tg_message.post_author
            existing_post_db.updated_at = datetime.now(timezone.utc)
            
            db.add(existing_post_db); post_for_comments_scan_candidate = existing_post_db; updated_posts_count_channel += 1
        else: 
            post_for_comments_scan_candidate = existing_post_db
            if existing_post_db.comments_count != api_comments_count:
                logger.debug(f"{log_prefix} Пост TG ID {existing_post_db.telegram_post_id}: comments_count обновлен с {existing_post_db.comments_count} на {api_comments_count} (update_existing_info_flag=False).")
                existing_post_db.comments_count = api_comments_count
                existing_post_db.updated_at = datetime.now(timezone.utc)
                db.add(existing_post_db)

        if post_for_comments_scan_candidate: posts_for_comment_scan_candidates.append(post_for_comments_scan_candidate)

    return posts_for_comment_scan_candidates, newly_created_post_objects, new_posts_count_channel, updated_posts_count_channel, latest_post_id_tg_seen_this_run

# --- ЗАДАЧИ CELERY ---

@celery_instance.task(name="add")
def add(x, y):
    logger.info(f"Тестовый таск 'add': {x} + {y}")
    time.sleep(5)
    result = x + y
    logger.info(f"Результат 'add': {result}")
    return result

@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str):
    logger.info(f"Тестовый таск 'simple_debug_task' получил: {message}")
    time.sleep(3)
    return f"Сообщение '{message}' обработано"

@celery_instance.task(name="collect_telegram_data", bind=True, max_retries=3, default_retry_delay=60 * 5)
def collect_telegram_data_task(self):
    task_start_time = time.time(); log_prefix = "[CollectDataTask]"; logger.info(f"{log_prefix} Запущен Celery таск '{self.name}' (ID: {self.request.id})...")
    api_id_val = settings.TELEGRAM_API_ID; api_hash_val = settings.TELEGRAM_API_HASH; phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    if not all([api_id_val, api_hash_val, phone_number_val]):
        logger.error(f"{log_prefix} Ошибка: Telegram API credentials не настроены.")
        return "Config error: Telegram API credentials"
    session_file_path = "/app/celery_telegram_session"

    async def _async_collect_data_logic():
        tg_client = None; total_ch_proc, total_new_p, total_upd_p, total_new_c = 0,0,0,0
        all_new_comment_ids_task_total: List[int] = []
        local_engine = None
        try:
            ASYNC_DB_URL_TASK = settings.DATABASE_URL 
            if not ASYNC_DB_URL_TASK.startswith("postgresql+asyncpg://"):
                ASYNC_DB_URL_TASK = ASYNC_DB_URL_TASK.replace("postgresql://", "postgresql+asyncpg://", 1)
            
            local_engine = create_async_engine(ASYNC_DB_URL_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory_Task = sessionmaker(
                bind=local_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False
            )
            logger.info(f"{log_prefix} Локальный async_engine и AsyncSessionFactory созданы для задачи {self.name}.")

            async with LocalAsyncSessionFactory_Task() as db: 
                tg_client = TelegramClient(session_file_path, api_id_val, api_hash_val)
                await tg_client.connect()
                if not await tg_client.is_user_authorized():
                    raise ConnectionRefusedError(f"Пользователь не авторизован для {session_file_path}.session")
                me = await tg_client.get_me()
                logger.info(f"{log_prefix} TGClient подключен как: {me.first_name if me else 'N/A'}")
                active_channels_db_result = await db.execute(select(Channel).where(Channel.is_active == True))
                active_channels_db: List[Channel] = active_channels_db_result.scalars().all()

                if not active_channels_db:
                    logger.info(f"{log_prefix} Нет активных каналов.")
                    return "Нет активных каналов."

                for idx, channel_db in enumerate(active_channels_db):
                    total_ch_proc +=1
                    logger.info(f"{log_prefix} Обработка канала: {channel_db.title} (ID: {channel_db.id}) ({idx+1}/{len(active_channels_db)})")
                    try:
                        tg_channel_entity = await tg_client.get_entity(channel_db.id)
                        if not isinstance(tg_channel_entity, TelethonChannelType) or not (getattr(tg_channel_entity, 'broadcast', False) or getattr(tg_channel_entity, 'megagroup', False)):
                            logger.warning(f"{log_prefix}  Канал {channel_db.id} невалиден. Деактивируем.")
                            channel_db.is_active = False; db.add(channel_db)
                            continue
                        iter_params: Dict[str, Any] = {"entity": tg_channel_entity, "limit": settings.POST_FETCH_LIMIT}
                        if channel_db.last_processed_post_id:
                            iter_params["min_id"] = channel_db.last_processed_post_id
                        elif settings.INITIAL_POST_FETCH_START_DATETIME:
                            iter_params["offset_date"] = settings.INITIAL_POST_FETCH_START_DATETIME
                            iter_params["reverse"] = True

                        # update_existing_info_flag=False, т.к. это сбор только новых постов
                        processed_posts_list, newly_created_posts, new_p_ch, _, last_id_tg = await _helper_fetch_and_process_posts_for_channel(
                            tg_client, db, channel_db, iter_params, 
                            update_existing_info_flag=False, 
                            log_prefix=log_prefix
                        )
                        total_new_p += new_p_ch
                        if last_id_tg > (channel_db.last_processed_post_id or 0):
                            channel_db.last_processed_post_id = last_id_tg
                            db.add(channel_db)

                        # Собираем комментарии только для НОВЫХ постов
                        if newly_created_posts:
                            logger.info(f"{log_prefix}  Сбор комментариев для {len(newly_created_posts)} новых постов...")
                            for post_obj in newly_created_posts:
                                num_c, new_c_ids = await _helper_fetch_and_process_comments_for_post(
                                    tg_client, db, post_obj, tg_channel_entity, 
                                    settings.COMMENT_FETCH_LIMIT, log_prefix=log_prefix
                                )
                                total_new_c += num_c
                                all_new_comment_ids_task_total.extend(new_c_ids)
                                # После сбора комментов, Post.comments_count в БД для этого post_obj НЕ обновляется здесь,
                                # т.к. _helper_fetch_and_process_comments_for_post больше не делает этого.
                                # Post.comments_count был установлен из API при создании поста в _helper_fetch_and_process_posts_for_channel.
                                # Это корректно для НОВЫХ постов.

                    except (ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError) as e_ch_access:
                        logger.warning(f"{log_prefix}  Канал {channel_db.id} ('{channel_db.title}') недоступен: {e_ch_access}. Деактивируем.")
                        channel_db.is_active = False; db.add(channel_db)
                        continue
                    except FloodWaitError as fwe_ch:
                        logger.warning(f"{log_prefix}  FloodWait ({fwe_ch.seconds} сек.) для канала {channel_db.title}. Пропускаем.")
                        await asyncio.sleep(fwe_ch.seconds + 10)
                        continue
                    except Exception as e_ch_proc:
                        logger.error(f"{log_prefix}  Ошибка обработки канала '{channel_db.title}': {type(e_ch_proc).__name__} - {e_ch_proc}", exc_info=True)
                        continue

                    if idx < len(active_channels_db) - 1:
                        logger.debug(f"{log_prefix} Пауза 1 сек перед обработкой следующего канала.")
                        await asyncio.sleep(1)
                await db.commit()
                summary = f"Сбор данных завершен. Каналов: {total_ch_proc}, Новых постов: {total_new_p}, Новых комм. собрано (только для новых постов): {total_new_c}."
                logger.info(f"{log_prefix} {summary}")
                
                if all_new_comment_ids_task_total:
                    unique_comment_ids_for_ai = sorted(list(set(all_new_comment_ids_task_total)))
                    logger.info(f"{log_prefix} Запуск AI-анализа для {len(unique_comment_ids_for_ai)} новых комментариев из collect_telegram_data_task.")
                    
                    ai_analysis_sub_batch_size = settings.COMMENT_ENQUEUE_BATCH_SIZE 
                    num_sub_tasks = 0
                    for i in range(0, len(unique_comment_ids_for_ai), ai_analysis_sub_batch_size):
                        batch_comment_ids = unique_comment_ids_for_ai[i:i + ai_analysis_sub_batch_size]
                        logger.info(f"{log_prefix}  Ставлю в очередь на AI-анализ задачу enqueue_comments_for_ai_feature_analysis_task с {len(batch_comment_ids)} ID комментариев.")
                        enqueue_comments_for_ai_feature_analysis_task.delay(
                            comment_ids_to_process=batch_comment_ids
                        )
                        num_sub_tasks += 1
                    logger.info(f"{log_prefix} AI-анализ для {len(unique_comment_ids_for_ai)} комментариев поставлен в очередь ({num_sub_tasks} задач(и) enqueue_comments_for_ai_feature_analysis_task).")

                return summary
        except ConnectionRefusedError as e_auth:
            logger.error(f"{log_prefix} ОШИБКА АВТОРИЗАЦИИ TELETHON: {e_auth}", exc_info=True); raise
        except Exception as e_main:
            logger.error(f"{log_prefix} КРИТИЧЕСКАЯ ОШИБКА: {type(e_main).__name__} - {e_main}", exc_info=True); raise
        finally:
            if tg_client and tg_client.is_connected():
                await tg_client.disconnect()
            if local_engine:
                logger.info(f"{log_prefix} Закрытие локального async_engine в finally для задачи {self.name}.")
                await local_engine.dispose()
    try:
        result = asyncio.run(_async_collect_data_logic())
        logger.info(f"{log_prefix} Таск '{self.name}' успешно завершен за {time.time() - task_start_time:.2f} сек. Результат: {result}")
        return result
    except ConnectionRefusedError as e_final_auth:
        logger.error(f"{log_prefix} ОШИБКА АВТОРИЗАЦИИ (не ретраим): {e_final_auth}", exc_info=True)
        raise e_final_auth
    except Exception as e_final_task:
        logger.error(f"{log_prefix} КРИТИЧЕСКАЯ ОШИБКА ЗАДАЧИ (ретрай): {type(e_final_task).__name__} - {e_final_task}", exc_info=True)
        try:
            if self.request.retries < self.max_retries:
                default_retry_delay_val = self.default_retry_delay if isinstance(self.default_retry_delay, (int, float)) else 300
                countdown = int(default_retry_delay_val * (2 ** self.request.retries))
                logger.info(f"Celery: Retry ({self.request.retries + 1}/{self.max_retries}) таска {self.request.id} через {countdown} сек")
                raise self.retry(exc=e_final_task, countdown=countdown)
            else:
                logger.error(f"Celery: Max retries ({self.max_retries}) достигнуто для таска {self.request.id}.")
                raise e_final_task
        except Exception as e_retry_logic:
            logger.error(f"Celery: Исключение в логике retry для таска {self.request.id}: {type(e_retry_logic).__name__}", exc_info=True)
            raise e_final_task

@celery_instance.task(name="summarize_posts_batch", bind=True, max_retries=2, default_retry_delay=300)
def summarize_posts_batch_task(
    self,
    channel_ids: Optional[List[int]] = None,
    start_date_iso: Optional[str] = None,
    end_date_iso: Optional[str] = None,
):
    limit_posts_to_process = settings.POST_SUMMARY_BATCH_SIZE
    task_start_time = time.time()
    log_prefix_parts = ["[PostSummaryTask"]
    if channel_ids: log_prefix_parts.append(f"Channels:{channel_ids}")
    if start_date_iso: log_prefix_parts.append(f"Start:{start_date_iso}")
    if end_date_iso: log_prefix_parts.append(f"End:{end_date_iso}")
    log_prefix = "".join(log_prefix_parts) + "]"

    logger.info(f"{log_prefix} Запущен Celery таск '{self.name}' (ID: {self.request.id}). Лимит пачки: {limit_posts_to_process}.")

    if not settings.OPENAI_API_KEY:
        logger.error(f"{log_prefix} Ошибка: OPENAI_API_KEY не настроен.")
        self.update_state(state='FAILURE', meta={'current_step': 'Ошибка конфигурации', 'error': 'OpenAI API Key не настроен.', 'progress': 0, 'processed_count':0, 'total_to_process':0})
        return "Config error: OpenAI Key not configured." # Этот return все еще актуален, так как задача не может продолжиться

    progress_info_ref: Dict[str, Any] = {
        "processed_count": 0,
        "total_to_process": 0,
        "current_step": "Инициализация задачи суммаризации",
        "progress": 0
    }

    async def _async_logic(task_instance, current_progress_info_ref: Dict[str, Any]):
        processed_count_in_batch = 0 # Фактически суммаризированных или помеченных как обработанные
        total_posts_for_batch = 0
        local_engine = None

        current_progress_info_ref.update({
            'current_step': 'Подготовка к суммаризации постов',
            'progress': 5,
            'processed_count': 0,
            'total_to_process': 0
        })
        task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)

        try:
            ASYNC_DB_URL_TASK = settings.DATABASE_URL
            if not ASYNC_DB_URL_TASK.startswith("postgresql+asyncpg://"):
                ASYNC_DB_URL_TASK = ASYNC_DB_URL_TASK.replace("postgresql://", "postgresql+asyncpg://", 1)

            local_engine = create_async_engine(ASYNC_DB_URL_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory_Task = sessionmaker(
                bind=local_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False
            )
            logger.info(f"{log_prefix} Локальный async_engine и AsyncSessionFactory созданы для задачи.")

            async with LocalAsyncSessionFactory_Task() as db_session:
                stmt = (
                    select(Post)
                    .where(Post.summary_text.is_(None))
                    .where(or_(Post.text_content.isnot(None), Post.caption_text.isnot(None))) # Есть что суммаризировать
                    .order_by(Post.posted_at.asc())
                    .limit(limit_posts_to_process)
                )

                if channel_ids:
                    logger.info(f"{log_prefix} Суммаризация для каналов: {channel_ids}")
                    stmt = stmt.where(Post.channel_id.in_(channel_ids))
                else:
                    logger.info(f"{log_prefix} Суммаризация для всех активных каналов (если даты не указаны).")
                    if not start_date_iso and not end_date_iso:
                         active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery()
                         stmt = stmt.join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id)

                if start_date_iso:
                    dt_start_naive = date.fromisoformat(start_date_iso.split('T')[0])
                    dt_start = datetime(dt_start_naive.year, dt_start_naive.month, dt_start_naive.day, 0, 0, 0, 0, tzinfo=timezone.utc)
                    stmt = stmt.where(Post.posted_at >= dt_start)
                    logger.info(f"{log_prefix} Применен фильтр start_date: {dt_start}")
                if end_date_iso:
                    dt_end_naive = date.fromisoformat(end_date_iso.split('T')[0])
                    dt_end = datetime(dt_end_naive.year, dt_end_naive.month, dt_end_naive.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
                    stmt = stmt.where(Post.posted_at <= dt_end)
                    logger.info(f"{log_prefix} Применен фильтр end_date: {dt_end}")

                posts_to_process_result = await db_session.execute(stmt)
                posts_to_process = posts_to_process_result.scalars().all()
                total_posts_for_batch = len(posts_to_process)
                current_progress_info_ref['total_to_process'] = total_posts_for_batch

                logger.info(f"{log_prefix} Найдено {total_posts_for_batch} постов для суммаризации в этой пачке.")
                current_progress_info_ref.update({
                    'current_step': f'Найдено {total_posts_for_batch} постов для суммаризации',
                    'progress': 10
                })
                task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)

                if not posts_to_process:
                    logger.info(f"{log_prefix} Не найдено постов для суммаризации в текущей пачке.")
                    result_message = "Нет постов для суммаризации в этой пачке."
                    current_progress_info_ref.update({
                        'current_step': 'Завершено: нет постов для суммаризации',
                        'progress': 100,
                        'result_summary': result_message
                    })
                    task_instance.update_state(state='SUCCESS', meta=current_progress_info_ref)
                    return result_message

                for i, post_obj in enumerate(posts_to_process):
                    text_to_summarize = post_obj.caption_text if post_obj.caption_text and post_obj.caption_text.strip() else post_obj.text_content
                    
                    # Пропускаем суммаризацию для слишком коротких постов или постов без текста
                    # но помечаем их как обработанные (с пустым резюме), чтобы не выбирать снова
                    if not text_to_summarize or len(text_to_summarize.strip()) < (settings.MIN_POST_LENGTH_FOR_SUMMARY or 30):
                        logger.info(f"{log_prefix}  Пост ID {post_obj.id} ({post_obj.link}) слишком короткий или без текста. Суммаризация пропущена, помечаем как обработанный (пустым резюме).")
                        post_obj.summary_text = "" # Пустая строка означает, что пост был рассмотрен, но не суммаризирован
                        post_obj.updated_at = datetime.now(timezone.utc)
                        db_session.add(post_obj)
                        processed_count_in_batch += 1
                        current_progress_info_ref['processed_count'] = processed_count_in_batch
                    else:
                        logger.info(f"{log_prefix}  Суммаризация поста ID {post_obj.id} ({post_obj.link})...")
                        try:
                            summary_prompt = f"Текст поста:\n---\n{text_to_summarize[:settings.LLM_MAX_PROMPT_LENGTH]}\n---\nНапиши краткое резюме (1-3 предложения на русском) основной мысли этого поста."
                            summary = await одиночный_запрос_к_llm(
                                summary_prompt,
                                модель=settings.OPENAI_DEFAULT_MODEL_FOR_TASKS or "gpt-3.5-turbo",
                                температура=0.3,
                                макс_токены=settings.LLM_SUMMARY_MAX_TOKENS or 250, # Используем настройку или дефолт
                                is_json_response_expected=False
                            )
                            if summary and summary.strip():
                                post_obj.summary_text = summary.strip()
                                post_obj.updated_at = datetime.now(timezone.utc)
                                db_session.add(post_obj)
                                processed_count_in_batch += 1
                                current_progress_info_ref['processed_count'] = processed_count_in_batch
                                logger.info(f"{log_prefix}    Резюме для поста ID {post_obj.id} получено и сохранено.")
                            else:
                                logger.warning(f"{log_prefix}    LLM не вернул текст резюме для поста ID {post_obj.id}. Помечаем как обработанный (пустым резюме).")
                                post_obj.summary_text = "" # Пустая строка
                                post_obj.updated_at = datetime.now(timezone.utc)
                                db_session.add(post_obj)
                                processed_count_in_batch += 1 # Все равно считаем обработанным
                                current_progress_info_ref['processed_count'] = processed_count_in_batch

                        except OpenAIError as e_llm:
                            logger.error(f"{log_prefix}    !!! Ошибка OpenAI API при суммаризации поста ID {post_obj.id}: {type(e_llm).__name__} - {e_llm}")
                            # Пропускаем этот пост, он останется без резюме для этой попытки
                            continue
                        except Exception as e_sum:
                            logger.error(f"{log_prefix}    !!! Неожиданная ошибка при суммаризации поста ID {post_obj.id}: {type(e_sum).__name__} - {e_sum}", exc_info=True)
                            continue # Пропускаем этот пост

                    # Обновление состояния прогресса периодически или в конце
                    if (i + 1) % 10 == 0 or (i + 1) == total_posts_for_batch:
                        current_progress_percentage = 10 + int(((i + 1) / total_posts_for_batch) * 85) if total_posts_for_batch > 0 else 95
                        current_progress_info_ref.update({
                            'current_step': f'Суммаризация: обработано {processed_count_in_batch}/{total_posts_for_batch} (просмотрено {i+1})',
                            'progress': current_progress_percentage
                        })
                        task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)
                
                # Коммит, если были какие-либо изменения (суммаризированные или помеченные как обработанные пустые/короткие посты)
                if processed_count_in_batch > 0: # processed_count_in_batch инкрементируется в обоих случаях (успех LLM или пропуск короткого)
                    await db_session.commit()
                    logger.info(f"{log_prefix}  Обработано (суммаризировано/пропущено) {processed_count_in_batch} постов в этой пачке.")

            result_message = f"Суммаризация для пачки завершена. Обработано (суммаризировано/пропущено): {processed_count_in_batch} из {total_posts_for_batch} постов."
            current_progress_info_ref.update({
                'current_step': 'Завершено',
                'progress': 100,
                'result_summary': result_message
            })
            task_instance.update_state(state='SUCCESS', meta=current_progress_info_ref)
            return result_message
        except Exception as e_async_main:
            logger.error(f"{log_prefix} !!! КРИТИЧЕСКАЯ ОШИБКА в _async_logic: {type(e_async_main).__name__} - {e_async_main}", exc_info=True)
            current_progress_info_ref.update({
                'current_step': f'Критическая ошибка в async_logic: {type(e_async_main).__name__}',
                'error': str(e_async_main)
            })
            raise
        finally:
            if local_engine:
                logger.info(f"{log_prefix} Закрытие локального async_engine в finally.")
                await local_engine.dispose()

    try:
        result_message = asyncio.run(_async_logic(self, progress_info_ref))
        task_duration = time.time() - task_start_time
        logger.info(f"{log_prefix} Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_main:
        task_duration = time.time() - task_start_time
        logger.error(f"{log_prefix} !!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {type(e_task_main).__name__} - {e_task_main}", exc_info=True)

        failure_meta = {
            'current_step': progress_info_ref.get('current_step', 'Ошибка суммаризации'),
            'error': str(e_task_main),
            'progress': progress_info_ref.get('progress', 0),
            'processed_count': progress_info_ref.get('processed_count', 0),
            'total_to_process': progress_info_ref.get('total_to_process', 0)
        }
        failure_meta['progress'] = 100
        failure_meta['current_step'] = f"Ошибка: {failure_meta['current_step']}"

        self.update_state(state='FAILURE', meta=failure_meta)

        try:
            if self.request.retries < self.max_retries:
                logger.info(f"{log_prefix} Попытка повтора задачи {self.request.id} ({self.request.retries + 1}/{self.max_retries}). Задержка: {int(self.default_retry_delay * (2 ** self.request.retries))} сек.")
                raise self.retry(exc=e_task_main, countdown=int(self.default_retry_delay * (2 ** self.request.retries)))
            else:
                logger.error(f"{log_prefix} Celery: Max retries ({self.max_retries}) достигнуто для таска {self.request.id}.")
                # Исключение e_task_main "выпадет", Celery обработает его.
        except Exception as e_retry_logic:
            logger.error(f"{log_prefix} Celery: Исключение в логике retry для таска {self.request.id}: {type(e_retry_logic).__name__}", exc_info=True)
            self.update_state(state='FAILURE', meta={**failure_meta, 'error': f"Retry logic failed: {str(e_retry_logic)}. Original error: {str(e_task_main)}"})
            raise

@celery_instance.task(name="send_daily_digest", bind=True, max_retries=3, default_retry_delay=180)
def send_daily_digest_task(self, hours_ago_posts=24, top_n_metrics=3):
    task_start_time = time.time(); log_prefix = "[DigestTask]"; logger.info(f"{log_prefix} Запущен Celery таск '{self.name}' (ID: {self.request.id})...")
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_TARGET_CHAT_ID:
        error_msg = "Ошибка: TELEGRAM_BOT_TOKEN или TELEGRAM_TARGET_CHAT_ID не настроены."
        logger.error(error_msg)
        return error_msg
        
    local_engine = None

    async def _async_send_digest_logic():
        nonlocal local_engine
        bot = telegram.Bot(token=settings.TELEGRAM_BOT_TOKEN); message_parts = []
        try:
            ASYNC_DB_URL_TASK = settings.DATABASE_URL 
            if not ASYNC_DB_URL_TASK.startswith("postgresql+asyncpg://"):
                ASYNC_DB_URL_TASK = ASYNC_DB_URL_TASK.replace("postgresql://", "postgresql+asyncpg://", 1)
            
            local_engine = create_async_engine(ASYNC_DB_URL_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory_Task = sessionmaker(
                bind=local_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False
            )
            logger.info(f"{log_prefix} Локальный async_engine и AsyncSessionFactory созданы для задачи {self.name}.")

            async with LocalAsyncSessionFactory_Task() as db_session:
                time_threshold_posts = datetime.now(timezone.utc) - timedelta(hours=hours_ago_posts)
                message_parts.append(helpers.escape_markdown(f" digest for Insight-Compass за последние {hours_ago_posts} часа:\n", version=2))
                active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery("active_channels_sq_digest")
                stmt_new_posts_count = (select(func.count(Post.id)).join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id).where(Post.posted_at >= time_threshold_posts))
                new_posts_count = (await db_session.execute(stmt_new_posts_count)).scalar_one_or_none() or 0
                message_parts.append(helpers.escape_markdown(f"📰 Всего новых постов (из активных каналов): {new_posts_count}\n", version=2))

                async def get_top_posts_by_metric(metric_column_or_expr_name: str, metric_display_name: str, is_sum_from_jsonb_flag=False):
                    top_posts_list = []; p_alias = aliased(Post, name=f"p_digest_{''.join(filter(str.isalnum, metric_display_name.lower()))}")
                    select_fields = [p_alias.link, p_alias.summary_text, p_alias.post_sentiment_label, Channel.title.label("channel_title")]; stmt_top = select(*select_fields); order_by_col = None
                    if is_sum_from_jsonb_flag:
                        rx_elements_cte = (select(p_alias.id.label("post_id_for_reactions"), cast(func.jsonb_array_elements(p_alias.reactions).op('->>')('count'), SAInteger).label("reaction_item_count")).select_from(p_alias).where(p_alias.reactions.isnot(None)).where(func.jsonb_typeof(p_alias.reactions) == 'array').cte(f"rx_elements_cte_{''.join(filter(str.isalnum, metric_display_name.lower()))}"))
                        sum_reactions_cte = (select(rx_elements_cte.c.post_id_for_reactions.label("post_id"), func.sum(rx_elements_cte.c.reaction_item_count).label("metric_value_for_digest")).group_by(rx_elements_cte.c.post_id_for_reactions).cte(f"sum_rx_cte_{''.join(filter(str.isalnum, metric_display_name.lower()))}"))
                        stmt_top = stmt_top.add_columns(sum_reactions_cte.c.metric_value_for_digest).outerjoin(sum_reactions_cte, p_alias.id == sum_reactions_cte.c.post_id); order_by_col = sum_reactions_cte.c.metric_value_for_digest
                    else:
                        actual_metric_column = getattr(p_alias, metric_column_or_expr_name)
                        stmt_top = stmt_top.add_columns(actual_metric_column.label("metric_value_for_digest")); order_by_col = actual_metric_column
                    stmt_top = stmt_top.join(Channel, p_alias.channel_id == Channel.id).where(Channel.is_active == True).where(p_alias.posted_at >= time_threshold_posts).where(p_alias.summary_text.isnot(None)).order_by(order_by_col.desc().nullslast()).limit(top_n_metrics)
                    for row in (await db_session.execute(stmt_top)).all():
                        top_posts_list.append({"link": row.link, "summary": row.summary_text, "sentiment": row.post_sentiment_label, "channel_title": row.channel_title, "metric_value": row.metric_value_for_digest})
                    return top_posts_list

                tops_to_include = [
                    {"metric_name": "comments_count", "label": "💬 Топ по комментариям", "emoji": "🗣️", "unit": "Комм."},
                    {"metric_name": "views_count", "label": "👀 Топ по просмотрам", "emoji": "👁️", "unit": "Просмотров"},
                    {"metric_name": "reactions", "label": "❤️ Топ по сумме реакций", "emoji": "👍", "unit": "Реакций", "is_sum_jsonb": True}
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
                            s_emoji = "😐 "; s_label_text = helpers.escape_markdown("N/A", version=2)
                            if p_data["sentiment"]:
                                s_label_text = helpers.escape_markdown(p_data["sentiment"].capitalize(), version=2)
                                if p_data["sentiment"] == "positive":
                                    s_emoji = "😊 "
                                elif p_data["sentiment"] == "negative":
                                    s_emoji = "😠 "
                            message_parts.append(f"\n{i+1}\\. {channel_md} [{helpers.escape_markdown('Пост',version=2)}]({link_md})\n   {top_conf['emoji']} {helpers.escape_markdown(top_conf['unit'], version=2)}: {metric_val_md} {s_emoji}{s_label_text}\n   📝 _{summary_md}_\n")
            digest_message_final = "".join(message_parts)
            if len(digest_message_final) > 4096:
                digest_message_final = digest_message_final[:4090] + helpers.escape_markdown("...", version=2)
                logger.warning("  ВНИМАНИЕ: Дайджест был обрезан.")
            logger.info(f"  Финальное сообщение дайджеста (начало):\n---\n{digest_message_final[:500]}...\n---")
            await bot.send_message(chat_id=settings.TELEGRAM_TARGET_CHAT_ID, text=digest_message_final, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
            return f"Расширенный дайджест успешно отправлен."
        except Exception as e_digest_logic:
            logger.error(f"!!! Ошибка в _async_send_digest_logic: {type(e_digest_logic).__name__} - {e_digest_logic}", exc_info=True)
            raise
        finally:
            if local_engine:
                logger.info(f"{log_prefix} Закрытие локального async_engine в finally для задачи {self.name}.")
                await local_engine.dispose()

    try:
        result_message = asyncio.run(_async_send_digest_logic()); task_duration = time.time() - task_start_time; logger.info(f"{log_prefix} Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except Exception as e_task_digest:
        task_duration = time.time() - task_start_time; logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {type(e_task_digest).__name__} - {e_task_digest}", exc_info=True)
        try:
            if self.request.retries < self.max_retries:
                default_retry_delay_val = self.default_retry_delay if isinstance(self.default_retry_delay, (int, float)) else 180
                countdown = int(default_retry_delay_val * (2 ** self.request.retries))
                logger.info(f"Celery: Retry ({self.request.retries + 1}/{self.max_retries}) таска {self.request.id} через {countdown} сек")
                raise self.retry(exc=e_task_digest, countdown=countdown)
            else:
                logger.error(f"Celery: Max retries ({self.max_retries}) достигнуто для таска {self.request.id}.")
                raise e_task_digest
        except Exception as e_retry_logic:
            logger.error(f"Celery: Исключение в логике retry для таска {self.request.id}: {type(e_retry_logic).__name__}", exc_info=True)
            raise e_retry_logic

@celery_instance.task(name="analyze_posts_sentiment", bind=True, max_retries=2, default_retry_delay=300)
def analyze_posts_sentiment_task(
    self,
    channel_ids: Optional[List[int]] = None,
    start_date_iso: Optional[str] = None,
    end_date_iso: Optional[str] = None,
):
    limit_posts_to_process = settings.POST_ANALYSIS_BATCH_SIZE
    task_start_time = time.time()
    log_prefix_parts = ["[PostSentimentTask"]
    if channel_ids: log_prefix_parts.append(f"Channels:{channel_ids}")
    if start_date_iso: log_prefix_parts.append(f"Start:{start_date_iso}")
    if end_date_iso: log_prefix_parts.append(f"End:{end_date_iso}")
    log_prefix = "".join(log_prefix_parts) + "]"

    logger.info(f"{log_prefix} Запущен Celery таск '{self.name}' (ID: {self.request.id}). Лимит пачки: {limit_posts_to_process}.")

    if not settings.OPENAI_API_KEY:
        logger.error(f"{log_prefix} Ошибка: OPENAI_API_KEY не настроен.")
        self.update_state(state='FAILURE', meta={'current_step': 'Ошибка конфигурации', 'error': 'OpenAI API Key не настроен.', 'progress': 0, 'processed_count':0, 'total_to_process':0})
        return "Config error: OpenAI Key not configured."

    # progress_info будет обновляться _async_logic и использоваться основной задачей для состояния FAILURE
    progress_info_ref: Dict[str, Any] = {
        "processed_count": 0,
        "total_to_process": 0,
        "current_step": "Инициализация задачи",
        "progress": 0
    }

    async def _async_logic(task_instance, current_progress_info_ref: Dict[str, Any]):
        analyzed_count_in_batch = 0
        total_posts_for_batch = 0
        local_engine = None

        current_progress_info_ref.update({
            'current_step': 'Подготовка к анализу тональности постов',
            'progress': 5,
            'processed_count': 0,
            'total_to_process': 0
        })
        task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)

        try:
            ASYNC_DB_URL_TASK = settings.DATABASE_URL
            if not ASYNC_DB_URL_TASK.startswith("postgresql+asyncpg://"):
                ASYNC_DB_URL_TASK = ASYNC_DB_URL_TASK.replace("postgresql://", "postgresql+asyncpg://", 1)

            local_engine = create_async_engine(ASYNC_DB_URL_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory_Task = sessionmaker(
                bind=local_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False
            )
            logger.info(f"{log_prefix} Локальный async_engine и AsyncSessionFactory созданы для задачи.")

            async with LocalAsyncSessionFactory_Task() as db_session:
                stmt = (
                    select(Post)
                    .where(or_(Post.text_content.isnot(None), Post.caption_text.isnot(None)))
                    .where(Post.post_sentiment_label.is_(None))
                    .order_by(Post.posted_at.asc())
                    .limit(limit_posts_to_process)
                )

                if channel_ids:
                    logger.info(f"{log_prefix} Анализ для каналов: {channel_ids}")
                    stmt = stmt.where(Post.channel_id.in_(channel_ids))
                else:
                    logger.info(f"{log_prefix} Анализ для всех активных каналов (если даты не указаны).")
                    if not start_date_iso and not end_date_iso:
                         active_channels_subquery = select(Channel.id).where(Channel.is_active == True).subquery()
                         stmt = stmt.join(active_channels_subquery, Post.channel_id == active_channels_subquery.c.id)

                if start_date_iso:
                    # ИЗМЕНЕНО: используется date.fromisoformat
                    dt_start_naive = date.fromisoformat(start_date_iso.split('T')[0])
                    dt_start = datetime(dt_start_naive.year, dt_start_naive.month, dt_start_naive.day, 0, 0, 0, 0, tzinfo=timezone.utc)
                    stmt = stmt.where(Post.posted_at >= dt_start)
                    logger.info(f"{log_prefix} Применен фильтр start_date: {dt_start}")
                if end_date_iso:
                    # ИЗМЕНЕНО: используется date.fromisoformat
                    dt_end_naive = date.fromisoformat(end_date_iso.split('T')[0])
                    dt_end = datetime(dt_end_naive.year, dt_end_naive.month, dt_end_naive.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
                    stmt = stmt.where(Post.posted_at <= dt_end)
                    logger.info(f"{log_prefix} Применен фильтр end_date: {dt_end}")

                posts_to_process_result = await db_session.execute(stmt)
                posts_to_process = posts_to_process_result.scalars().all()
                total_posts_for_batch = len(posts_to_process)
                current_progress_info_ref['total_to_process'] = total_posts_for_batch

                logger.info(f"{log_prefix} Найдено {total_posts_for_batch} постов для анализа в этой пачке.")
                current_progress_info_ref.update({
                    'current_step': f'Найдено {total_posts_for_batch} постов для анализа',
                    'progress': 10,
                })
                task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)


                if not posts_to_process:
                    logger.info(f"{log_prefix} Не найдено постов для анализа тональности в текущей пачке.")
                    result_message = "Нет постов для анализа в этой пачке."
                    current_progress_info_ref.update({
                        'current_step': 'Завершено: нет постов для анализа',
                        'progress': 100,
                        'result_summary': result_message
                    })
                    task_instance.update_state(state='SUCCESS', meta=current_progress_info_ref)
                    return result_message

                for i, post_obj in enumerate(posts_to_process):
                    text_for_analysis = post_obj.caption_text if post_obj.caption_text and post_obj.caption_text.strip() else post_obj.text_content
                    if not text_for_analysis or not text_for_analysis.strip():
                        logger.info(f"{log_prefix}  Пост ID {post_obj.id} ({post_obj.link}) не имеет текста/подписи или текст пустой. Помечаем как 'neutral' без вызова LLM.")
                        post_obj.post_sentiment_label = "neutral"
                        post_obj.post_sentiment_score = 0.0
                        post_obj.updated_at = datetime.now(timezone.utc)
                        db_session.add(post_obj)
                        analyzed_count_in_batch += 1 # Считаем как обработанный, хотя LLM не вызывался
                        current_progress_info_ref['processed_count'] = analyzed_count_in_batch
                        # LLM не вызывался, поэтому переходим к логике обновления прогресса
                    else:
                        logger.info(f"{log_prefix}  Анализ тональности поста ID {post_obj.id} ({post_obj.link})...")
                        s_label, s_score = "neutral", 0.0 # По умолчанию
                        try:
                            prompt = f"Определи тональность текста (JSON: sentiment_label: [positive,negative,neutral,mixed], sentiment_score: [-1.0,1.0]):\n---\n{text_for_analysis[:settings.LLM_MAX_PROMPT_LENGTH]}\n---\nJSON_RESPONSE:"
                            llm_response_str = await одиночный_запрос_к_llm(prompt, модель=settings.OPENAI_DEFAULT_MODEL_FOR_TASKS or "gpt-3.5-turbo-1106", температура=0.2, макс_токены=60, is_json_response_expected=True)
                            if llm_response_str:
                                try:
                                    data = json.loads(llm_response_str)
                                    s_label_candidate = data.get("sentiment_label")
                                    s_score_candidate_raw = data.get("sentiment_score")
                                    if s_label_candidate in ["positive", "negative", "neutral", "mixed"]: s_label = s_label_candidate
                                    else: logger.warning(f"{log_prefix}    LLM вернул невалидный sentiment_label '{s_label_candidate}' для поста ID {post_obj.id}. Установлен 'neutral'. Ответ: {llm_response_str}"); s_label = "neutral"
                                    if isinstance(s_score_candidate_raw, (int, float)) and -1.0 <= float(s_score_candidate_raw) <= 1.0: s_score = float(s_score_candidate_raw)
                                    else: logger.warning(f"{log_prefix}    LLM вернул некорректный sentiment_score '{s_score_candidate_raw}' для поста ID {post_obj.id}. Установлен 0.0. Ответ: {llm_response_str}"); s_score = 0.0
                                    if s_label_candidate is None and s_score_candidate_raw is None : s_label = "neutral" # Оба отсутствуют, считаем neutral
                                except (json.JSONDecodeError, TypeError, ValueError) as e_json: logger.error(f"{log_prefix}    Ошибка парсинга JSON от LLM для поста ID {post_obj.id} ({type(e_json).__name__}: {e_json}). Ответ LLM: '{llm_response_str}'")
                            else: logger.warning(f"{log_prefix}    LLM вернул пустой ответ для анализа тональности поста ID {post_obj.id}. Устанавливаем 'neutral'.")
                        except OpenAIError as e_llm_sentiment:
                            logger.error(f"{log_prefix}    !!! Ошибка OpenAI API при анализе тональности поста ID {post_obj.id}: {type(e_llm_sentiment).__name__} - {e_llm_sentiment}");
                            # Пропускаем этот пост, он останется без анализа тональности для этой попытки
                            # Обновление прогресса произойдет на следующей итерации или в конце
                            continue
                        except Exception as e_sa_general:
                            logger.error(f"{log_prefix}    !!! Неожиданная ошибка при анализе тональности поста ID {post_obj.id}: {type(e_sa_general).__name__} - {e_sa_general}", exc_info=True);
                            continue # Пропускаем этот пост

                        post_obj.post_sentiment_label = s_label
                        post_obj.post_sentiment_score = s_score
                        post_obj.updated_at = datetime.now(timezone.utc)
                        db_session.add(post_obj)
                        analyzed_count_in_batch += 1
                        current_progress_info_ref['processed_count'] = analyzed_count_in_batch
                        logger.info(f"{log_prefix}    Тональность поста ID {post_obj.id}: {s_label} ({s_score:.2f}) сохранена.")

                    # Обновление состояния прогресса периодически или в конце
                    # i - индекс текущего поста, analyzed_count_in_batch - сколько фактически проанализировано (или помечено neutral)
                    # Используем i+1 для проверки "каждый 10-й *просмотренный* пост"
                    if (i + 1) % 10 == 0 or (i + 1) == total_posts_for_batch:
                        # Прогресс считаем от общего числа постов, которые *должны быть* обработаны
                        current_progress_percentage = 10 + int(((i + 1) / total_posts_for_batch) * 85) if total_posts_for_batch > 0 else 95
                        current_progress_info_ref.update({
                            'current_step': f'Анализ тональности: обработано {analyzed_count_in_batch}/{total_posts_for_batch} (просмотрено {i+1})',
                            'progress': current_progress_percentage
                        })
                        task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)

                # Коммит, если были какие-либо изменения (проанализированные или помеченные как neutral пустые посты)
                if analyzed_count_in_batch > 0 or any(
                    (p.post_sentiment_label == "neutral" and not p.text_content and not p.caption_text) for p in posts_to_process
                ):
                    await db_session.commit()
                    logger.info(f"{log_prefix}  Обновлено {current_progress_info_ref['processed_count']} постов в этой пачке (включая помеченные neutral).")


            result_message = f"Анализ тональности для пачки завершен. Обработано (с LLM или помечено neutral): {current_progress_info_ref['processed_count']} из {total_posts_for_batch} постов."
            current_progress_info_ref.update({
                'current_step': 'Завершено',
                'progress': 100,
                'result_summary': result_message
            })
            task_instance.update_state(state='SUCCESS', meta=current_progress_info_ref)
            return result_message
        except Exception as e_async_main:
            logger.error(f"{log_prefix} !!! КРИТИЧЕСКАЯ ОШИБКА в _async_logic: {type(e_async_main).__name__} - {e_async_main}", exc_info=True)
            current_progress_info_ref.update({
                'current_step': f'Критическая ошибка в async_logic: {type(e_async_main).__name__}',
                'error': str(e_async_main)
            })
            raise
        finally:
            if local_engine:
                logger.info(f"{log_prefix} Закрытие локального async_engine в finally.")
                await local_engine.dispose()

    try:
        result_message = asyncio.run(_async_logic(self, progress_info_ref))
        task_duration = time.time() - task_start_time
        logger.info(f"{log_prefix} Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}")
        # Состояние SUCCESS уже установлено внутри _async_logic
        return result_message
    except Exception as e_task_main:
        task_duration = time.time() - task_start_time
        logger.error(f"{log_prefix} !!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {type(e_task_main).__name__} - {e_task_main}", exc_info=True)

        failure_meta = {
            'current_step': progress_info_ref.get('current_step', 'Ошибка анализа тональности'),
            'error': str(e_task_main),
            'progress': progress_info_ref.get('progress', 0),
            'processed_count': progress_info_ref.get('processed_count', 0),
            'total_to_process': progress_info_ref.get('total_to_process', 0)
        }
        failure_meta['progress'] = 100 # Задача дошла до конца (с ошибкой)
        failure_meta['current_step'] = f"Ошибка: {failure_meta['current_step']}"


        self.update_state(state='FAILURE', meta=failure_meta)

        try:
            if self.request.retries < self.max_retries:
                logger.info(f"{log_prefix} Попытка повтора задачи {self.request.id} ({self.request.retries + 1}/{self.max_retries}). Задержка: {int(self.default_retry_delay * (2 ** self.request.retries))} сек.")
                raise self.retry(exc=e_task_main, countdown=int(self.default_retry_delay * (2 ** self.request.retries)))
            else:
                logger.error(f"{log_prefix} Celery: Max retries ({self.max_retries}) достигнуто для таска {self.request.id}.")
                # ИЗМЕНЕНО: убран return. Celery сам обработает статус FAILURE.
                # Метаданные уже установлены через self.update_state.
                # Если здесь будет return, статус может стать SUCCESS.
                # Исключение e_task_main "выпадет" из задачи, и Celery обработает его.
        except Exception as e_retry_logic:
            logger.error(f"{log_prefix} Celery: Исключение в логике retry для таска {self.request.id}: {type(e_retry_logic).__name__}", exc_info=True)
            # Обновляем состояние, если сама логика retry дала сбой
            self.update_state(state='FAILURE', meta={**failure_meta, 'error': f"Retry logic failed: {str(e_retry_logic)}. Original error: {str(e_task_main)}"})
            # Позволяем оригинальному или новому исключению "выпасть"
            raise # Перевыбрасываем, чтобы Celery корректно завершил задачу с FAILURE

@celery_instance.task(name="tasks.analyze_single_comment_ai_features", bind=True, max_retries=2, default_retry_delay=60 * 2)
def analyze_single_comment_ai_features_task(self, comment_id: int):
    task_start_time = time.time()
    log_prefix = "[AICommentFeaturesMock]" # Изменили префикс для ясности
    logger.info(f"{log_prefix} Запущена 'заглушка' для анализа comment_id: {comment_id} (Task ID: {self.request.id}). LLM ВЫЗОВ ОТКЛЮЧЕН.")
    
    local_engine = None 
    
    async def _async_analyze_comment_logic():
        nonlocal local_engine
        try:
            ASYNC_DB_URL_TASK = settings.DATABASE_URL 
            if not ASYNC_DB_URL_TASK.startswith("postgresql+asyncpg://"):
                ASYNC_DB_URL_TASK = ASYNC_DB_URL_TASK.replace("postgresql://", "postgresql+asyncpg://", 1)
            
            local_engine = create_async_engine(ASYNC_DB_URL_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory_Task = sessionmaker(
                bind=local_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False
            )

            async with LocalAsyncSessionFactory_Task() as db_session:
                # Просто проверяем существование комментария по ID
                comment_exists_id = await db_session.scalar(
                    select(Comment.id).where(Comment.id == comment_id)
                )

                if not comment_exists_id: # Если scalar вернул None (комментарий не найден)
                    logger.warning(f"{log_prefix} Комментарий с ID {comment_id} не найден. Ничего не делаем.")
                    return f"Comment ID {comment_id} not found for mock processing."

                # Просто помечаем комментарий как "проанализированный" (без вызова LLM)
                # Поля extracted_topics и т.д. устанавливаются в пустые списки
                update_values = {
                    "ai_analysis_completed_at": datetime.now(timezone.utc),
                    "extracted_topics": [], 
                    "extracted_problems": [],
                    "extracted_questions": [],
                    "extracted_suggestions": []
                }
                update_stmt = update(Comment).where(Comment.id == comment_id).values(**update_values)
                await db_session.execute(update_stmt)
                await db_session.commit()
                logger.info(f"{log_prefix} Комментарий ID {comment_id} помечен как обработанный (LLM вызов пропущен).")
                return f"Comment ID {comment_id} mock-processed (LLM call skipped)."

        except Exception as e_general_comment_analysis:
            logger.error(f"{log_prefix} Общая ошибка при mock-обработке comment_id {comment_id}: {type(e_general_comment_analysis).__name__} - {e_general_comment_analysis}", exc_info=True)
            raise 
        finally:
            if local_engine:
                logger.info(f"{log_prefix} Закрытие локального async_engine в finally (comment_id: {comment_id}).")
                await local_engine.dispose()

    try:
        result_message = asyncio.run(_async_analyze_comment_logic())
        task_duration = time.time() - task_start_time
        logger.info(f"{log_prefix} Celery таск '{self.name}' (заглушка) завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_level:
        task_duration = time.time() - task_start_time; logger.error(f"{log_prefix} !!! Celery: КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (заглушка) (comment_id: {comment_id}) (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}", exc_info=True)
        try:
            if self.request.retries < self.max_retries:
                default_retry_delay_val = self.default_retry_delay if isinstance(self.default_retry_delay, (int, float)) else 120
                countdown = int(default_retry_delay_val * (2 ** self.request.retries))
                logger.info(f"Celery: Retry ({self.request.retries + 1}/{self.max_retries}) таска {self.request.id} (comment_id: {comment_id}) через {countdown} сек")
                raise self.retry(exc=e_task_level, countdown=countdown)
            else:
                logger.error(f"Celery: Max retries ({self.max_retries}) достигнуто для таска {self.request.id} (comment_id: {comment_id}).")
                raise e_task_level
        except Exception as e_retry_logic:
            logger.error(f"Celery: Исключение в логике retry для таска {self.request.id} (comment_id: {comment_id}): {type(e_retry_logic).__name__}", exc_info=True)
            raise e_task_level

@celery_instance.task(name="tasks.enqueue_comments_for_ai_feature_analysis", bind=True, max_retries=2, default_retry_delay=180) # Уменьшил default_retry_delay
def enqueue_comments_for_ai_feature_analysis_task(
    self,
    limit_comments_to_queue: Optional[int] = None,
    older_than_hours: Optional[int] = None,
    channel_id_filter: Optional[int] = None,
    process_only_recent_hours: Optional[int] = None,
    comment_ids_to_process: Optional[List[int]] = None,
    start_date_iso: Optional[str] = None, # Для фильтрации постов, к которым относятся комментарии
    end_date_iso: Optional[str] = None    # Для фильтрации постов, к которым относятся комментарии
):
    actual_limit_comments_to_queue = limit_comments_to_queue if limit_comments_to_queue is not None else settings.COMMENT_ENQUEUE_BATCH_SIZE
    task_start_time = time.time()
    log_prefix_parts = ["[AICommentQueue"]
    if channel_id_filter: log_prefix_parts.append(f"Ch:{channel_id_filter}")
    if start_date_iso: log_prefix_parts.append(f"StartPost:{start_date_iso}") # Уточнил, что это для постов
    if end_date_iso: log_prefix_parts.append(f"EndPost:{end_date_iso}")     # Уточнил, что это для постов
    if comment_ids_to_process: log_prefix_parts.append(f"IDs:{len(comment_ids_to_process)}")
    if older_than_hours is not None: log_prefix_parts.append(f"OlderThanH:{older_than_hours}")
    if process_only_recent_hours is not None: log_prefix_parts.append(f"RecentH:{process_only_recent_hours}")
    log_prefix = "".join(log_prefix_parts) + "]"

    logger.info(f"{log_prefix} Запуск задачи. Лимит пачки (если не заданы ID): {actual_limit_comments_to_queue} (Task ID: {self.request.id})")

    progress_info_ref: Dict[str, Any] = {
        "processed_count": 0, # Сколько комментариев было поставлено в очередь
        "total_to_process": 0, # Сколько комментариев было найдено для постановки в очередь
        "current_step": "Инициализация задачи постановки комментариев в очередь",
        "progress": 0
    }

    async def _async_enqueue_logic(task_instance, current_progress_info_ref: Dict[str, Any]):
        enqueued_count_local = 0 # Используем локальную переменную для _async_logic
        total_found_for_queue = 0
        local_engine = None

        current_progress_info_ref.update({
            'current_step': 'Подготовка к поиску комментариев для очереди',
            'progress': 5
        })
        task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)

        try:
            ASYNC_DB_URL_TASK = settings.DATABASE_URL
            if not ASYNC_DB_URL_TASK.startswith("postgresql+asyncpg://"):
                ASYNC_DB_URL_TASK = ASYNC_DB_URL_TASK.replace("postgresql://", "postgresql+asyncpg://", 1)

            local_engine = create_async_engine(ASYNC_DB_URL_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory_Task = sessionmaker(
                bind=local_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False
            )
            logger.info(f"{log_prefix} Локальный async_engine и AsyncSessionFactory созданы для задачи.")

            async with LocalAsyncSessionFactory_Task() as db_session:
                comment_ids_to_enqueue: List[int] = []
                if comment_ids_to_process is not None:
                    logger.info(f"{log_prefix} Режим обработки конкретных ID комментариев: {len(comment_ids_to_process)} шт.")
                    if comment_ids_to_process:
                        stmt_filter_ids = select(Comment.id)\
                            .where(Comment.id.in_(comment_ids_to_process))\
                            .where(Comment.text_content.isnot(None))\
                            .where(Comment.text_content != "")\
                            .where(Comment.ai_analysis_completed_at.is_(None)) # Только те, что еще не анализировались

                        filtered_ids_result = await db_session.execute(stmt_filter_ids)
                        comment_ids_to_enqueue = filtered_ids_result.scalars().all()
                        logger.info(f"{log_prefix} Из переданных {len(comment_ids_to_process)} ID, {len(comment_ids_to_enqueue)} подходят для AI-анализа и будут поставлены в очередь.")
                    else:
                        comment_ids_to_enqueue = []
                else:
                    # Базовый запрос для комментариев, которые еще не анализировались или устарели
                    stmt = select(Comment.id).where(Comment.text_content.isnot(None)).where(Comment.text_content != "")

                    # Присоединяем Post для фильтрации по каналу или датам постов
                    # Это нужно делать только если есть соответствующие фильтры
                    needs_join_post = bool(channel_id_filter or start_date_iso or end_date_iso)
                    if needs_join_post:
                        stmt = stmt.join(Post, Comment.post_id == Post.id)
                        if channel_id_filter:
                            stmt = stmt.where(Post.channel_id == channel_id_filter)

                        if start_date_iso:
                            dt_start_naive = date.fromisoformat(start_date_iso.split('T')[0])
                            dt_start = datetime(dt_start_naive.year, dt_start_naive.month, dt_start_naive.day, 0, 0, 0, 0, tzinfo=timezone.utc)
                            stmt = stmt.where(Post.posted_at >= dt_start)
                            logger.info(f"{log_prefix} Применен фильтр start_date для постов: {dt_start}")
                        if end_date_iso:
                            dt_end_naive = date.fromisoformat(end_date_iso.split('T')[0])
                            dt_end = datetime(dt_end_naive.year, dt_end_naive.month, dt_end_naive.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
                            stmt = stmt.where(Post.posted_at <= dt_end)
                            logger.info(f"{log_prefix} Применен фильтр end_date для постов: {dt_end}")

                    # Фильтры по времени комментирования и статусу анализа
                    if process_only_recent_hours is not None and process_only_recent_hours > 0:
                        time_threshold_recent = datetime.now(timezone.utc) - timedelta(hours=process_only_recent_hours)
                        stmt = stmt.where(Comment.commented_at >= time_threshold_recent)
                        stmt = stmt.where(Comment.ai_analysis_completed_at.is_(None)) # Только не проанализированные
                        stmt = stmt.order_by(Comment.commented_at.desc()) # Сначала самые новые
                        logger.info(f"{log_prefix} Режим недавних: за {process_only_recent_hours}ч, только непроанализированные.")
                    else:
                        if older_than_hours is not None:
                            # Анализируем те, что не анализировались ИЛИ те, чей анализ устарел
                            time_threshold_older = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
                            stmt = stmt.where(
                                or_(
                                    Comment.ai_analysis_completed_at.is_(None),
                                    Comment.ai_analysis_completed_at < time_threshold_older
                                )
                            )
                            logger.info(f"{log_prefix} Режим бэклога с переанализом старых (старше {older_than_hours}ч).")
                        else:
                            # Только те, что еще не анализировались
                            stmt = stmt.where(Comment.ai_analysis_completed_at.is_(None))
                            logger.info(f"{log_prefix} Режим бэклога (только непроанализированные).")
                        stmt = stmt.order_by(Comment.commented_at.asc()) # Сначала самые старые из подходящих

                    stmt = stmt.limit(actual_limit_comments_to_queue)
                    comment_ids_result = await db_session.execute(stmt)
                    comment_ids_to_enqueue = comment_ids_result.scalars().all()

                total_found_for_queue = len(comment_ids_to_enqueue)
                current_progress_info_ref['total_to_process'] = total_found_for_queue

                logger.info(f"{log_prefix} Найдено {total_found_for_queue} комментариев для очереди.")
                current_progress_info_ref.update({
                    'current_step': f'Найдено {total_found_for_queue} комментариев для постановки в очередь',
                    'progress': 20 # Увеличили начальный прогресс
                })
                task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)


                if not comment_ids_to_enqueue:
                    logger.info(f"{log_prefix} Не найдено комментариев для очереди по заданным критериям.")
                    result_message = "No comments found to queue."
                    current_progress_info_ref.update({
                        'current_step': 'Завершено: нет комментариев для очереди',
                        'progress': 100,
                        'result_summary': result_message
                    })
                    task_instance.update_state(state='SUCCESS', meta=current_progress_info_ref)
                    return result_message

                logger.info(f"{log_prefix} Начинаю постановку в очередь {total_found_for_queue} комментариев...")
                for i, comment_id_to_process in enumerate(comment_ids_to_enqueue):
                    analyze_single_comment_ai_features_task.delay(comment_id_to_process)
                    enqueued_count_local += 1
                    current_progress_info_ref['processed_count'] = enqueued_count_local

                    if (i + 1) % 20 == 0 or (i + 1) == total_found_for_queue: # Обновляем каждые 20 или в конце
                        current_progress_percentage = 20 + int(((i + 1) / total_found_for_queue) * 75) if total_found_for_queue > 0 else 95
                        current_progress_info_ref.update({
                            'current_step': f'Постановка в очередь: {enqueued_count_local}/{total_found_for_queue}',
                            'progress': current_progress_percentage
                        })
                        task_instance.update_state(state='PROGRESS', meta=current_progress_info_ref)

                result_message = f"Successfully enqueued {enqueued_count_local} comments for detailed AI analysis."
                current_progress_info_ref.update({
                    'current_step': 'Завершено',
                    'progress': 100,
                    'result_summary': result_message
                })
                task_instance.update_state(state='SUCCESS', meta=current_progress_info_ref)
                return result_message

        except Exception as e_enqueue:
            logger.error(f"{log_prefix} Ошибка в _async_enqueue_logic: {type(e_enqueue).__name__} - {e_enqueue}", exc_info=True)
            current_progress_info_ref.update({
                'current_step': f'Ошибка в async_logic: {type(e_enqueue).__name__}',
                'error': str(e_enqueue)
            })
            raise # Перевыбрасываем для обработки в основной части задачи
        finally:
            if local_engine:
                logger.info(f"{log_prefix} Закрытие локального async_engine в finally.")
                await local_engine.dispose()

    try:
        result_message = asyncio.run(_async_enqueue_logic(self, progress_info_ref))
        task_duration = time.time() - task_start_time
        logger.info(f"{log_prefix} Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}")
        # Состояние SUCCESS уже установлено внутри _async_logic
        return result_message
    except Exception as e_task_level:
        task_duration = time.time() - task_start_time
        logger.error(f"{log_prefix} !!! Celery: КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}", exc_info=True)
        
        failure_meta = {
            'current_step': progress_info_ref.get('current_step', 'Ошибка постановки комментариев в очередь'),
            'error': str(e_task_level),
            'progress': progress_info_ref.get('progress', 0),
            'processed_count': progress_info_ref.get('processed_count', 0),
            'total_to_process': progress_info_ref.get('total_to_process', 0)
        }
        failure_meta['progress'] = 100 # Задача дошла до конца (с ошибкой)
        failure_meta['current_step'] = f"Ошибка: {failure_meta['current_step']}"

        self.update_state(state='FAILURE', meta=failure_meta)

        try:
            if self.request.retries < self.max_retries:
                logger.info(f"{log_prefix} Попытка повтора задачи {self.request.id} ({self.request.retries + 1}/{self.max_retries}). Задержка: {int(self.default_retry_delay * (2 ** self.request.retries))} сек.")
                raise self.retry(exc=e_task_level, countdown=int(self.default_retry_delay * (2 ** self.request.retries)))
            else:
                logger.error(f"{log_prefix} Celery: Max retries ({self.max_retries}) достигнуто для таска {self.request.id}.")
                # Исключение e_task_level "выпадет", Celery обработает его.
        except Exception as e_retry_logic: # Ловим исключения из self.retry
            logger.error(f"{log_prefix} Celery: Исключение в логике retry для таска {self.request.id}: {type(e_retry_logic).__name__}", exc_info=True)
            # Убеждаемся, что состояние FAILURE установлено, если сама логика retry падает
            self.update_state(state='FAILURE', meta={**failure_meta, 'error': f"Retry logic failed: {str(e_retry_logic)}. Original error: {str(e_task_level)}"})
            raise # Перевыбрасываем, чтобы Celery корректно завершил задачу с FAILURE

@celery_instance.task(name="tasks.advanced_data_refresh", bind=True, max_retries=2, default_retry_delay=60 * 10)
def advanced_data_refresh_task(
    self,
    channel_ids: Optional[List[int]] = None,
    post_refresh_mode_str: str = PostRefreshMode.NEW_ONLY.value,
    post_refresh_days: Optional[int] = None, 
    post_refresh_start_date_iso: Optional[str] = None,
    post_limit_per_channel: int = 100,
    update_existing_posts_info: bool = False,
    comment_refresh_mode_str: str = CommentRefreshMode.ADD_NEW_TO_EXISTING.value,
    comment_limit_per_post: int = settings.COMMENT_FETCH_LIMIT,
    analyze_new_comments: bool = True
):
    task_start_time = time.time()
    log_prefix = "[AdvancedRefresh]"
    
    logger.info(f"{log_prefix} Запущена задача (с ЛОКАЛЬНЫМ ENGINE). ID: {self.request.id}. Параметры: channels={channel_ids}, post_mode='{post_refresh_mode_str}', post_days={post_refresh_days}, post_start_date='{post_refresh_start_date_iso}', post_limit={post_limit_per_channel}, update_existing={update_existing_posts_info}, comment_mode='{comment_refresh_mode_str}', comment_limit={comment_limit_per_post}, analyze={analyze_new_comments}")
    
    self.update_state(state='PROGRESS', meta={'current_step': 'Инициализация задачи (Локальный Engine)', 'progress': 5})
    
    post_refresh_mode_enum: PostRefreshMode
    comment_refresh_mode_enum: CommentRefreshMode
    post_refresh_start_date_dt: Optional[datetime] = None # Переименовано для ясности
    
    try:
        post_refresh_mode_enum = PostRefreshMode(post_refresh_mode_str)
        comment_refresh_mode_enum = CommentRefreshMode(comment_refresh_mode_str)
        
        if post_refresh_start_date_iso:
            # Убедимся, что парсим только дату, время будет 00:00:00 UTC
            parsed_date_obj = date.fromisoformat(post_refresh_start_date_iso.split('T')[0])
            post_refresh_start_date_dt = datetime(parsed_date_obj.year, parsed_date_obj.month, parsed_date_obj.day, tzinfo=timezone.utc)
            logger.info(f"{log_prefix} post_refresh_start_date_dt установлен: {post_refresh_start_date_dt.isoformat()}")

    except ValueError as e:
        logger.error(f"{log_prefix} Ошибка преобразования параметров: {e}")
        self.update_state(state='FAILURE', meta={'current_step': 'Ошибка параметров задачи', 'progress': 100, 'error': str(e)})
        return f"Ошибка параметров задачи: {e}"
    
    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    session_file_path = "/app/celery_telegram_session"

    if not all([api_id_val, api_hash_val]): 
        logger.error(f"{log_prefix} Ошибка: Telegram API ID/Hash не настроены.")
        self.update_state(state='FAILURE', meta={'current_step': 'Ошибка конфигурации Telegram API', 'progress': 100, 'error': 'Credentials (ID/Hash) not configured'})
        return "Config error: Telegram API ID/Hash"

    async def _async_advanced_refresh_logic():
        tg_client = None
        local_engine = None 
        processed_channels_count = 0
        total_new_posts_task = 0
        total_updated_posts_info_task = 0 # Общий счетчик постов, у которых обновилась информация
        total_new_comments_collected_task = 0
        newly_added_comment_ids_for_ai_task: List[int] = [] # Только ID новых комментов для AI
        
        try:
            ASYNC_DB_URL = settings.DATABASE_URL 
            if not ASYNC_DB_URL.startswith("postgresql+asyncpg://"):
                ASYNC_DB_URL = ASYNC_DB_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
            
            local_engine = create_async_engine(ASYNC_DB_URL, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(
                bind=local_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, autocommit=False
            )
            logger.info(f"{log_prefix} Локальный async_engine и AsyncSessionFactory созданы для задачи.")

            async with LocalAsyncSessionFactory() as db: 
                logger.info(f"{log_prefix} Создание TGClient: {session_file_path}")
                tg_client = TelegramClient(session_file_path, api_id_val, api_hash_val)
                self.update_state(state='PROGRESS', meta={'current_step': 'Подключение к Telegram', 'progress': 10})
                await tg_client.connect()
                if not await tg_client.is_user_authorized():
                    self.update_state(state='FAILURE', meta={'current_step': 'Ошибка авторизации Telegram', 'error': 'TG Client not authorized'})
                    raise ConnectionRefusedError(f"Celery: Пользователь не авторизован для {session_file_path}.session")
                me = await tg_client.get_me()
                logger.info(f"{log_prefix} TGClient подключен как: {me.first_name if me else 'N/A'}")
                
                channels_to_process_q = select(Channel).where(Channel.is_active == True)
                if channel_ids is not None:
                    if not any(channel_ids): 
                        logger.info(f"{log_prefix} Передан пустой список ID каналов.")
                        self.update_state(state='SUCCESS', meta={'current_step': 'Завершено (пустой список ID каналов)', 'progress': 100, 'result_summary': 'Пустой список ID каналов для обработки.'})
                        return "Пустой список ID каналов для обработки."
                    channels_to_process_q = channels_to_process_q.where(Channel.id.in_(channel_ids))
                
                channels_result = await db.execute(channels_to_process_q)
                channels_db_list: List[Channel] = channels_result.scalars().all()

                if not channels_db_list:
                    logger.info(f"{log_prefix} Нет каналов для обработки.")
                    self.update_state(state='SUCCESS', meta={'current_step': 'Завершено (нет каналов для обработки)', 'progress': 100, 'result_summary': 'Нет каналов для обработки.'})
                    return "Нет каналов для обработки."
                
                total_channels_to_process = len(channels_db_list)
                logger.info(f"{log_prefix} Найдено {total_channels_to_process} каналов для обработки.")
                base_progress = 15

                for idx, channel_db_obj in enumerate(channels_db_list):
                    processed_channels_count += 1
                    channel_progress = base_progress + int(((idx + 1) / total_channels_to_process) * 70) 
                    self.update_state(state='PROGRESS', meta={'current_step': f'Канал: {channel_db_obj.title} ({idx+1}/{total_channels_to_process})', 'progress': channel_progress, 'channel_id': channel_db_obj.id, 'channel_title': channel_db_obj.title})
                    logger.info(f"{log_prefix} Обработка канала: '{channel_db_obj.title}' (ID: {channel_db_obj.id})")
                    
                    # Счетчики для текущего канала
                    channel_new_posts = 0
                    channel_updated_posts_info = 0
                    channel_new_comments = 0
                    
                    try:
                        tg_channel_entity = await tg_client.get_entity(channel_db_obj.id)
                        
                        posts_to_scan_comments_for: List[Post] = [] 
                        
                        latest_post_id_tg_for_channel_update = channel_db_obj.last_processed_post_id or 0

                        if post_refresh_mode_enum == PostRefreshMode.UPDATE_STATS_ONLY:
                            logger.info(f"{log_prefix}  Режим UPDATE_STATS_ONLY для канала {channel_db_obj.title}.")
                            
                            comment_count_subquery = (
                                select(
                                    Comment.post_id,
                                    func.count(Comment.id).label("db_comment_count")
                                )
                                .group_by(Comment.post_id)
                                .subquery("comment_counts")
                            )
                            db_posts_stmt = (
                                select(Post, func.coalesce(comment_count_subquery.c.db_comment_count, 0).label("db_comment_count_for_post"))
                                .outerjoin(comment_count_subquery, Post.id == comment_count_subquery.c.post_id)
                                .where(Post.channel_id == channel_db_obj.id)
                                .order_by(Post.telegram_post_id.desc()) # Сначала самые новые для обновления
                            )
                            
                            if post_refresh_days:
                                date_limit = datetime.now(timezone.utc) - timedelta(days=post_refresh_days)
                                db_posts_stmt = db_posts_stmt.where(Post.posted_at >= date_limit)
                                logger.info(f"{log_prefix}    UPDATE_STATS_ONLY: применен фильтр по post_refresh_days ({post_refresh_days} дней)")
                            elif post_refresh_start_date_dt:
                                db_posts_stmt = db_posts_stmt.where(Post.posted_at >= post_refresh_start_date_dt)
                                logger.info(f"{log_prefix}    UPDATE_STATS_ONLY: применен фильтр по post_refresh_start_date ({post_refresh_start_date_dt.isoformat()})")
                            
                            if post_limit_per_channel and post_limit_per_channel > 0 :
                                db_posts_stmt = db_posts_stmt.limit(post_limit_per_channel)
                                logger.info(f"{log_prefix}    UPDATE_STATS_ONLY: применен лимит {post_limit_per_channel} постов из БД для обновления.")

                            db_posts_data_list_result = await db.execute(db_posts_stmt)
                            db_posts_data_list = db_posts_data_list_result.all() 

                            if db_posts_data_list:
                                telegram_post_ids_to_fetch = [p_data[0].telegram_post_id for p_data in db_posts_data_list]
                                logger.info(f"{log_prefix}    Найдено {len(db_posts_data_list)} постов в БД для канала {channel_db_obj.title} для обновления статистики (согласно фильтрам).")
                                
                                batch_size = 100 
                                for i in range(0, len(telegram_post_ids_to_fetch), batch_size):
                                    batch_ids = telegram_post_ids_to_fetch[i:i + batch_size]
                                    fetched_tg_messages: Optional[List[Message]] = None
                                    logger.info(f"{log_prefix}    Запрашиваем из Telegram информацию для пачки из {len(batch_ids)} постов...")
                                    try:
                                        messages_or_none = await tg_client.get_messages(tg_channel_entity, ids=batch_ids)
                                        if messages_or_none:
                                            fetched_tg_messages = [msg for msg in messages_or_none if msg is not None and not isinstance(msg, MessageService)] 
                                    except Exception as e_get_msgs_batch:
                                        logger.error(f"{log_prefix}    Ошибка при пакетном получении сообщений по ID для канала {channel_db_obj.id}: {e_get_msgs_batch}")
                                        continue 

                                    if fetched_tg_messages:
                                        logger.info(f"{log_prefix}      Получено {len(fetched_tg_messages)} НЕ сервисных сообщений из Telegram для обновления.")
                                        
                                        db_posts_map_with_counts = {
                                            p_data[0].telegram_post_id: (p_data[0], p_data[1]) 
                                            for p_data in db_posts_data_list 
                                            if p_data[0].telegram_post_id in batch_ids
                                        }

                                        for tg_message in fetched_tg_messages:
                                            if tg_message.id in db_posts_map_with_counts:
                                                post_in_db, db_comment_count = db_posts_map_with_counts[tg_message.id]
                                                
                                                api_comments_count_tg = tg_message.replies.replies if tg_message.replies and tg_message.replies.replies is not None else 0
                                                
                                                info_changed_for_post = False
                                                if update_existing_posts_info: 
                                                    if post_in_db.views_count != tg_message.views: info_changed_for_post = True; post_in_db.views_count = tg_message.views
                                                    new_reactions = await _process_reactions_for_db(tg_message.reactions)
                                                    if post_in_db.reactions != new_reactions : info_changed_for_post = True; post_in_db.reactions = new_reactions
                                                    if post_in_db.forwards_count != tg_message.forwards: info_changed_for_post = True; post_in_db.forwards_count = tg_message.forwards
                                                    new_edited_at = tg_message.edit_date.replace(tzinfo=timezone.utc) if tg_message.edit_date else None
                                                    if post_in_db.edited_at != new_edited_at: info_changed_for_post = True; post_in_db.edited_at = new_edited_at
                                                    
                                                    new_text, new_caption = (None, tg_message.text) if tg_message.media and tg_message.text else (tg_message.text if not tg_message.media else None, None)
                                                    if post_in_db.text_content != new_text: info_changed_for_post = True; post_in_db.text_content = new_text
                                                    if post_in_db.caption_text != new_caption: info_changed_for_post = True; post_in_db.caption_text = new_caption
                                                    
                                                    new_media_type, new_media_info = await _process_media_for_db(tg_message.media)
                                                    if post_in_db.media_type != new_media_type or post_in_db.media_content_info != new_media_info:
                                                        info_changed_for_post = True; post_in_db.media_type = new_media_type; post_in_db.media_content_info = new_media_info
                                                    
                                                    if (tg_message.pinned or False) != post_in_db.is_pinned: info_changed_for_post = True; post_in_db.is_pinned = tg_message.pinned or False
                                                    if tg_message.post_author != post_in_db.author_signature: info_changed_for_post = True; post_in_db.author_signature = tg_message.post_author
                                                    
                                                    if info_changed_for_post:
                                                        channel_updated_posts_info +=1
                                                        post_in_db.updated_at = datetime.now(timezone.utc)
                                                
                                                # Обновляем счетчик комментов в Post всегда, если он отличается от TG
                                                if post_in_db.comments_count != api_comments_count_tg:
                                                    logger.debug(f"{log_prefix}      Пост TG ID {post_in_db.telegram_post_id}: comments_count обновлен с {post_in_db.comments_count} на {api_comments_count_tg} из Telegram API.")
                                                    post_in_db.comments_count = api_comments_count_tg
                                                    if not info_changed_for_post : # Если только счетчик комментов изменился
                                                        post_in_db.updated_at = datetime.now(timezone.utc)
                                                        # Не считаем это как channel_updated_posts_info, если update_existing_posts_info=False
                                                        # Но если update_existing_posts_info=True, то это уже посчитано выше
                                                
                                                db.add(post_in_db) 

                                                if api_comments_count_tg > db_comment_count:
                                                    logger.info(f"{log_prefix}      Пост TG ID {post_in_db.telegram_post_id}: счетчик комм. увеличился (API: {api_comments_count_tg}, DB было: {db_comment_count}). Ставим на сбор недостающих.")
                                                    posts_to_scan_comments_for.append(post_in_db)
                                                elif comment_refresh_mode_enum == CommentRefreshMode.ADD_NEW_TO_EXISTING and api_comments_count_tg > 0 :
                                                    # Если режим "добавлять новые к существующим", и в API есть комменты (даже если счетчик не вырос),
                                                    # то стоит проверить, не появились ли ID, которых нет у нас.
                                                    # Это может произойти, если кто-то удалил старый коммент и добавил новый, и общее число осталось тем же.
                                                    logger.info(f"{log_prefix}      Пост TG ID {post_in_db.telegram_post_id}: счетчик комм. не вырос ({api_comments_count_tg}), но режим ADD_NEW_TO_EXISTING. Ставим на проверку новых ID.")
                                                    posts_to_scan_comments_for.append(post_in_db)
                                                else:
                                                    logger.debug(f"{log_prefix}      Пост TG ID {post_in_db.telegram_post_id}: счетчик комм. не увеличился (API: {api_comments_count_tg}, DB было: {db_comment_count}). Сбор недостающих комм. не требуется.")
                            else:
                                logger.info(f"{log_prefix}    Нет постов в БД для канала {channel_db_obj.title} для обновления статистики (согласно фильтрам).")

                        else: # Режимы NEW_ONLY, LAST_N_DAYS, SINCE_DATE
                            iter_params_helper = {"entity": tg_channel_entity, "limit": post_limit_per_channel} # Лимит для helper'а
                            if post_refresh_mode_enum == PostRefreshMode.NEW_ONLY:
                                if channel_db_obj.last_processed_post_id:
                                    iter_params_helper["min_id"] = channel_db_obj.last_processed_post_id
                                else: # Если last_processed_post_id нет, но есть INITIAL_POST_FETCH_START_DATETIME, используем его
                                    if settings.INITIAL_POST_FETCH_START_DATETIME:
                                       iter_params_helper["offset_date"] = settings.INITIAL_POST_FETCH_START_DATETIME
                                       iter_params_helper["reverse"] = True
                                    # Если и его нет, helper будет собирать последние post_limit_per_channel постов
                            elif post_refresh_mode_enum == PostRefreshMode.LAST_N_DAYS and post_refresh_days:
                                iter_params_helper["offset_date"] = datetime.now(timezone.utc) - timedelta(days=post_refresh_days)
                                iter_params_helper["reverse"] = True
                            elif post_refresh_mode_enum == PostRefreshMode.SINCE_DATE and post_refresh_start_date_dt:
                                iter_params_helper["offset_date"] = post_refresh_start_date_dt
                                iter_params_helper["reverse"] = True
                            
                            all_processed_posts_from_helper, newly_created_posts_from_helper, num_new_p, num_upd_p, last_id_tg_helper = await _helper_fetch_and_process_posts_for_channel(
                                tg_client, db, channel_db_obj, iter_params_helper, 
                                update_existing_posts_info, log_prefix
                            )
                            channel_new_posts += num_new_p
                            channel_updated_posts_info += num_upd_p 
                            latest_post_id_tg_for_channel_update = max(latest_post_id_tg_for_channel_update, last_id_tg_helper)

                            if comment_refresh_mode_enum == CommentRefreshMode.NEW_POSTS_ONLY:
                                posts_to_scan_comments_for.extend(newly_created_posts_from_helper)
                            else: # ADD_NEW_TO_EXISTING
                                posts_to_scan_comments_for.extend(all_processed_posts_from_helper)
                        
                        if post_refresh_mode_enum == PostRefreshMode.NEW_ONLY and latest_post_id_tg_for_channel_update > (channel_db_obj.last_processed_post_id or 0):
                            channel_db_obj.last_processed_post_id = latest_post_id_tg_for_channel_update
                            db.add(channel_db_obj)

                        if comment_refresh_mode_enum != CommentRefreshMode.DO_NOT_REFRESH:
                            unique_posts_for_comment_scan = []
                            seen_post_ids_for_scan = set()
                            for post_obj in posts_to_scan_comments_for: # posts_to_scan_comments_for теперь содержит только те, что нужно
                                if post_obj.id not in seen_post_ids_for_scan:
                                    unique_posts_for_comment_scan.append(post_obj)
                                    seen_post_ids_for_scan.add(post_obj.id)

                            if unique_posts_for_comment_scan: 
                                logger.info(f"{log_prefix}  Сбор комментариев для {len(unique_posts_for_comment_scan)} постов канала {channel_db_obj.id} (режим комм: {comment_refresh_mode_enum.value})...")
                                for post_obj_to_scan in unique_posts_for_comment_scan:
                                    num_c, new_c_ids = await _helper_fetch_and_process_comments_for_post(
                                        tg_client, db, post_obj_to_scan, tg_channel_entity, 
                                        comment_limit_per_post, log_prefix
                                    )
                                    channel_new_comments += num_c
                                    newly_added_comment_ids_for_ai_task.extend(new_c_ids)
                            else:
                                logger.info(f"{log_prefix}  Нет постов для сбора комментариев в канале {channel_db_obj.id} (согласно выбранной логике).")
                        else:
                            logger.info(f"{log_prefix}  Сбор комментариев пропущен для канала {channel_db_obj.id} (режим комм: DO_NOT_REFRESH).")
                            
                    except (ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError) as e_ch_access:
                        logger.warning(f"{log_prefix}  Канал {channel_db_obj.id} ('{channel_db_obj.title}') недоступен: {e_ch_access}. Деактивируем.")
                        channel_db_obj.is_active = False; db.add(channel_db_obj)
                        continue 
                    except FloodWaitError as fwe_ch:
                        logger.warning(f"{log_prefix}  FloodWait ({fwe_ch.seconds} сек.) для канала {channel_db_obj.title}. Пропускаем канал в этом запуске.")
                        await asyncio.sleep(fwe_ch.seconds + 10) 
                        continue
                    except Exception as e_ch_proc:
                        logger.error(f"{log_prefix}  Ошибка обработки канала '{channel_db_obj.title}': {type(e_ch_proc).__name__} - {e_ch_proc}", exc_info=True)
                        continue 
                    
                    total_new_posts_task += channel_new_posts
                    total_updated_posts_info_task += channel_updated_posts_info
                    total_new_comments_collected_task += channel_new_comments
                    
                    if idx < total_channels_to_process - 1: 
                        logger.debug(f"{log_prefix} Пауза 1 сек перед обработкой следующего канала.")
                        await asyncio.sleep(1) 
                
                await db.commit() 
                
                final_summary = f"Обновление завершено. Каналов обработано: {processed_channels_count}, Новых постов: {total_new_posts_task}, Обновлено инфо о постах: {total_updated_posts_info_task}, Новых комментариев собрано: {total_new_comments_collected_task}."
                logger.info(f"{log_prefix} {final_summary}")
                
                current_meta = {'current_step': 'Данные собраны, подготовка к AI-анализу', 'progress': 85, 'summary_so_far': final_summary}
                self.update_state(state='PROGRESS', meta=current_meta)

                if analyze_new_comments and newly_added_comment_ids_for_ai_task:
                    unique_comment_ids = sorted(list(set(newly_added_comment_ids_for_ai_task)))
                    logger.info(f"{log_prefix} Запуск AI-анализа для {len(unique_comment_ids)} новых/обновленных комментариев.")
                    
                    ai_analysis_sub_batch_size = settings.COMMENT_ENQUEUE_BATCH_SIZE 
                    
                    num_sub_tasks = 0
                    for i in range(0, len(unique_comment_ids), ai_analysis_sub_batch_size):
                        batch_comment_ids = unique_comment_ids[i:i + ai_analysis_sub_batch_size]
                        logger.info(f"{log_prefix}  Ставлю в очередь на AI-анализ задачу enqueue_comments_for_ai_feature_analysis_task с {len(batch_comment_ids)} ID комментариев.")
                        enqueue_comments_for_ai_feature_analysis_task.delay(
                            comment_ids_to_process=batch_comment_ids
                        )
                        num_sub_tasks += 1

                    current_meta['current_step'] = f'AI-анализ для {len(unique_comment_ids)} комментариев поставлен в очередь ({num_sub_tasks} задач(и) enqueue_comments_for_ai_feature_analysis_task)'
                    current_meta['progress'] = 95
                    self.update_state(state='PROGRESS', meta=current_meta)

                elif analyze_new_comments:
                    logger.info(f"{log_prefix} Нет новых комментариев для AI-анализа.")
                    current_meta['current_step'] = 'Нет комментариев для AI-анализа'
                    current_meta['progress'] = 95
                    self.update_state(state='PROGRESS', meta=current_meta)
                
                self.update_state(state='SUCCESS', meta={'current_step': 'Завершено успешно!', 'progress': 100, 'result_summary': final_summary})
                return final_summary
        except ConnectionRefusedError as e_auth_tg: 
            logger.error(f"{log_prefix} ОШИБКА АВТОРИЗАЦИИ TELETHON: {e_auth_tg}", exc_info=True)
            self.update_state(state='FAILURE', meta={'current_step': 'Ошибка авторизации Telegram', 'error': str(e_auth_tg), 'progress': 100}) 
            raise 
        except Exception as e_main_refresh:
            logger.error(f"{log_prefix} КРИТИЧЕСКАЯ ОШИБКА в _async_advanced_refresh_logic: {type(e_main_refresh).__name__} - {e_main_refresh}", exc_info=True)
            self.update_state(state='FAILURE', meta={'current_step': f'Критическая ошибка: {type(e_main_refresh).__name__}', 'error': str(e_main_refresh), 'progress': 100})
            raise 
        finally:
            if tg_client and tg_client.is_connected():
                logger.info(f"{log_prefix} Отключение Telegram клиента в finally.")
                try:
                    await tg_client.disconnect()
                except Exception as e_disconnect: 
                    logger.error(f"{log_prefix} Ошибка при отключении tg_client: {e_disconnect}", exc_info=True)
            if local_engine:
                logger.info(f"{log_prefix} Закрытие локального async_engine в finally.")
                await local_engine.dispose()

    try:
        result_message = asyncio.run(_async_advanced_refresh_logic())
        task_duration = time.time() - task_start_time
        logger.info(f"{log_prefix} Celery таск '{self.name}' (с ЛОКАЛЬНЫМ ENGINE) завершен за {task_duration:.2f} сек. Результат: {result_message}")
        return result_message
    except Exception as e_task_level: 
        task_duration = time.time() - task_start_time
        logger.error(f"!!! Celery: ОШИБКА УРОВНЯ ЗАДАЧИ в '{self.name}' (с ЛОКАЛЬНЫМ ENGINE) (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}", exc_info=True)
        
        current_task_info = self.AsyncResult(self.request.id).info
        is_failure_already_set_specifically = isinstance(current_task_info, dict) and \
                                            self.AsyncResult(self.request.id).state == 'FAILURE' and \
                                            current_task_info.get('error') 

        if not is_failure_already_set_specifically:
             self.update_state(state='FAILURE', meta={'current_step': f'Общая ошибка выполнения задачи: {type(e_task_level).__name__}', 'error': str(e_task_level), 'progress': 100})
        
        if isinstance(e_task_level, ConnectionRefusedError): 
            logger.warning(f"Celery: НЕ БУДЕТ ПОВТОРА для {self.request.id} из-за ошибки авторизации.")
            raise e_task_level from None 
        
        try:
            if self.request.retries < self.max_retries:
                default_retry_delay_val = self.default_retry_delay if isinstance(self.default_retry_delay, (int, float)) else 600
                countdown = int(default_retry_delay_val * (2 ** self.request.retries))
                logger.info(f"Celery: Retry ({self.request.retries + 1}/{self.max_retries}) таска {self.request.id} (с ЛОКАЛЬНЫМ ENGINE) через {countdown} сек из-за: {type(e_task_level).__name__}")
                raise self.retry(exc=e_task_level, countdown=countdown)
            else:
                logger.error(f"Celery: Max retries ({self.max_retries}) достигнуто для {self.request.id} (с ЛОКАЛЬНЫМ ENGINE). Ошибка: {type(e_task_level).__name__}")
                raise e_task_level from None
        except Exception as e_retry_logic: 
            logger.error(f"Celery: Исключение в логике retry для {self.request.id} (с ЛОКАЛЬНЫМ ENGINE): {type(e_retry_logic).__name__}", exc_info=True)
            raise e_task_level from e_retry_logic