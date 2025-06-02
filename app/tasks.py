# app/tasks.py

import asyncio
import os
import time
import traceback
import json
from datetime import timezone, datetime, timedelta
from typing import List, Dict, Any, Optional

import openai
from openai import OpenAIError

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.future import select
from sqlalchemy import desc, func, update, cast, literal_column, nullslast, Integer as SAInteger # Убедимся, что все импорты есть
from sqlalchemy.dialects.postgresql import JSONB

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

from app.celery_app import celery_instance
from app.core.config import settings
from app.models.telegram_data import Channel, Post, Comment

# --- Существующие задачи (add, simple_debug_task) ---
@celery_instance.task(name="add")
def add(x, y): print(f"Тестовый таск 'add': {x} + {y}"); time.sleep(5); result = x + y; print(f"Результат 'add': {result}"); return result
@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str): print(f"Тестовый таск 'simple_debug_task' получил: {message}"); time.sleep(3); return f"Сообщение '{message}' обработано"

# --- Вспомогательные функции для collect_telegram_data_task ---
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
@celery_instance.task(name="collect_telegram_data", bind=True, max_retries=3, default_retry_delay=60 * 5)
def collect_telegram_data_task(self):
    task_start_time = time.time(); print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (РАСШИРЕННЫЙ сбор)...")
    api_id_val = settings.TELEGRAM_API_ID; api_hash_val = settings.TELEGRAM_API_HASH; phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    if not all([api_id_val, api_hash_val, phone_number_val]): print("Ошибка: Telegram API credentials не настроены."); return "Config error"
    session_file_path_in_container = "/app/celery_telegram_session"; print(f"Celery Worker session: {session_file_path_in_container}.session")
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
    async def _async_main_logic_collector():
        tg_client = None; local_async_engine = None; total_channels_processed, total_posts_collected, total_comments_collected = 0,0,0
        try:
            local_async_engine = create_async_engine(ASYNC_DB_URL_FOR_TASK, echo=False, pool_pre_ping=True)
            LocalAsyncSessionFactory = sessionmaker(bind=local_async_engine, class_=AsyncSession, expire_on_commit=False)
            print(f"Celery: Создание TGClient: {session_file_path_in_container}"); tg_client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val)
            print(f"Celery: Подключение к TG..."); await tg_client.connect()
            if not await tg_client.is_user_authorized(): raise ConnectionRefusedError(f"Celery: Пользователь не авторизован для {session_file_path_in_container}.session")
            me = await tg_client.get_me(); print(f"Celery: Успешно подключен как: {me.first_name} (@{me.username or ''})")
            async with LocalAsyncSessionFactory() as db_session:
                active_channels_from_db: List[Channel] = (await db_session.execute(select(Channel).where(Channel.is_active == True))).scalars().all()
                if not active_channels_from_db: print("Celery: Нет активных каналов."); return "Нет активных каналов."
                print(f"Celery: Найдено {len(active_channels_from_db)} активных каналов.")
                for channel_db_obj in active_channels_from_db:
                    print(f"\nCelery: Обработка канала: '{channel_db_obj.title}' (ID: {channel_db_obj.id})"); total_channels_processed += 1
                    newly_added_post_objects_in_session: list[Post] = []
                    try:
                        channel_entity_tg = await tg_client.get_entity(channel_db_obj.id)
                        if not isinstance(channel_entity_tg, TelethonChannelType) or not (getattr(channel_entity_tg, 'broadcast', False) or getattr(channel_entity_tg, 'megagroup', False)):
                            print(f"  Канал {channel_db_obj.id} невалиден. Деактивируем."); channel_db_obj.is_active = False; db_session.add(channel_db_obj); continue
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
                        if temp_posts_buffer_for_db_add: db_session.add_all(temp_posts_buffer_for_db_add); print(f"  Добавлено в сессию {collected_for_this_channel_this_run} постов.")
                        if latest_post_id_seen_this_run > (channel_db_obj.last_processed_post_id or 0): channel_db_obj.last_processed_post_id = latest_post_id_seen_this_run; db_session.add(channel_db_obj); print(f"  Обновлен last_id для канала: {latest_post_id_seen_this_run}")
                        if newly_added_post_objects_in_session:
                            print(f"  Celery: Сбор комментариев для {len(newly_added_post_objects_in_session)} новых постов..."); await db_session.flush(); comments_collected_channel_total = 0
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
                                        comm_user_id, comm_user_username, comm_user_fullname = (comment_msg.from_id.user_id, None, None) if isinstance(comment_msg.from_id, PeerUser) else (None, None, None)
                                        new_comm_obj = Comment(telegram_comment_id=comment_msg.id, post_id=post_obj_in_db.id, telegram_user_id=comm_user_id, user_username=comm_user_username, user_fullname=comm_user_fullname, text_content=comm_text or "", commented_at=comment_msg.date.replace(tzinfo=timezone.utc) if comment_msg.date else datetime.now(timezone.utc), reactions=comm_reactions, reply_to_telegram_comment_id=comm_reply_to_id, media_type=comm_media_type, media_content_info=comm_media_info, caption_text=comm_caption, edited_at=comment_msg.edit_date.replace(tzinfo=timezone.utc) if comment_msg.edit_date else None)
                                        db_session.add(new_comm_obj); current_post_comm_count +=1; total_comments_collected +=1
                                    if current_post_comm_count > 0: await db_session.execute(update(Post).where(Post.id == post_obj_in_db.id).values(comments_count=(Post.comments_count if Post.comments_count is not None else 0) + current_post_comm_count)); comments_collected_channel_total += current_post_comm_count
                                except TelethonMessageIdInvalidError: print(f"    Celery: Комментарии для поста {post_obj_in_db.telegram_post_id} не найдены (TMI).")
                                except FloodWaitError as fwe_comm: print(f"    !!! Celery: FloodWait комм. поста {post_obj_in_db.telegram_post_id}: {fwe_comm.seconds}s"); await asyncio.sleep(fwe_comm.seconds + 5)
                                except Exception as e_comm_loop: print(f"    Celery: НЕОЖИДАННАЯ ошибка комм. поста {post_obj_in_db.telegram_post_id}: {type(e_comm_loop).__name__} - {e_comm_loop}")
                            if comments_collected_channel_total > 0 : print(f"    Собрано {comments_collected_channel_total} комментариев для канала.")
                    except (ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError) as e_ch_access: print(f"  Канал {channel_db_obj.id} недоступен: {e_ch_access}. Деактивируем."); channel_db_obj.is_active = False; db_session.add(channel_db_obj)
                    except FloodWaitError as fwe_ch: print(f"  FloodWait для канала {channel_db_obj.title}: {fwe_ch.seconds} сек."); await asyncio.sleep(fwe_ch.seconds + 5)
                    except Exception as e_ch_proc: print(f"  Celery: Ошибка обработки канала '{channel_db_obj.title}': {type(e_ch_proc).__name__} - {e_ch_proc}"); traceback.print_exc(limit=1)
                await db_session.commit() ; print(f"\nCelery: Все изменения сохранены в БД. Обработано: {total_channels_processed} каналов.")
            return f"Celery: Сбор данных завершен успешно. Статистика: {total_channels_processed}ch, {total_posts_collected}p, {total_comments_collected}c."
        except ConnectionRefusedError as e_auth: print(f"!!! Celery: ОШИБКА АВТОРИЗАЦИИ TELETHON: {e_auth}"); raise
        except Exception as e_async_logic: print(f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_collector: {type(e_async_logic).__name__} {e_async_logic}"); traceback.print_exc(); raise
        finally:
            if tg_client and tg_client.is_connected(): print("Celery: Отключение Telegram..."); await tg_client.disconnect()
            if local_async_engine: print("Celery: Закрытие БД..."); await local_async_engine.dispose()
    try:
        result_message = asyncio.run(_async_main_logic_collector())
        task_duration = time.time() - task_start_time; print(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except ConnectionRefusedError as e_auth_final: 
        task_duration = time.time() - task_start_time
        print(f"!!! Celery: ОШИБКА АВТОРИЗАЦИИ в таске '{self.name}' (за {task_duration:.2f} сек): {e_auth_final}. НЕ ПОВТОРЯТЬ.")
        raise e_auth_final from e_auth_final
    except Exception as e_task_level: 
        task_duration = time.time() - task_start_time
        print(f"!!! Celery: КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {type(e_task_level).__name__} {e_task_level}")
        traceback.print_exc()
        try: 
            countdown = int(self.default_retry_delay * (2 ** self.request.retries))
            print(f"Celery: Retry ({self.request.retries + 1}/{self.max_retries}) таска {self.request.id} через {countdown} сек")
            raise self.retry(exc=e_task_level, countdown=countdown)
        except self.MaxRetriesExceededError: 
            print(f"Celery: Max retries для таска {self.request.id}. Ошибка: {e_task_level}")
            raise e_task_level from e_task_level
        except Exception as e_retry_logic: 
            print(f"Celery: Ошибка в логике retry: {e_retry_logic}")
            raise e_task_level from e_task_level

# --- ЗАДАЧА СУММАРИЗАЦИИ (AI) ---
@celery_instance.task(name="summarize_top_posts", bind=True, max_retries=2, default_retry_delay=300)
def summarize_top_posts_task(self, hours_ago=48, top_n=5):
    task_start_time = time.time(); print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (AI Суммаризация топ-{top_n} постов за {hours_ago}ч)...")
    if not settings.OPENAI_API_KEY: print("Ошибка: OPENAI_API_KEY не настроен."); return "Config error OpenAI"
    try: openai_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    except Exception as e_init_openai: print(f"Ошибка OpenAI init: {e_init_openai}"); raise self.retry(exc=e_init_openai)
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
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
                if not posts_to_process: print(f"  Не найдено постов для суммаризации."); return "Нет постов для суммаризации."
                print(f"  Найдено {len(posts_to_process)} постов для суммаризации.")
                for post_obj in posts_to_process:
                    text_to_summarize = post_obj.caption_text if post_obj.caption_text else post_obj.text_content
                    if not text_to_summarize or len(text_to_summarize.strip()) < 30 : 
                        print(f"    Пост ID {post_obj.id} слишком короткий или без текста/подписи, пропускаем."); continue
                    print(f"    Суммаризация поста ID {post_obj.id}...")
                    try:
                        summary_prompt = f"Текст поста:\n---\n{text_to_summarize[:4000]}\n---\nНапиши краткое резюме (1-3 предложения на русском) основной мысли этого поста."
                        completion = await asyncio.to_thread(openai_client.chat.completions.create, model="gpt-3.5-turbo", messages=[{"role": "system", "content": "Ты AI-ассистент, генерирующий краткие резюме текстов."}, {"role": "user", "content": summary_prompt}], temperature=0.3, max_tokens=200)
                        summary = completion.choices[0].message.content.strip()
                        if summary: post_obj.summary_text = summary; post_obj.updated_at = datetime.now(timezone.utc); db_session.add(post_obj); processed_posts_count += 1; print(f"      Резюме для поста ID {post_obj.id} получено.")
                    except OpenAIError as e_llm: print(f"    !!! Ошибка OpenAI API поста ID {post_obj.id}: {e_llm}")
                    except Exception as e_sum: print(f"    !!! Неожиданная ошибка суммаризации поста ID {post_obj.id}: {e_sum}"); traceback.print_exc(limit=1)
                if processed_posts_count > 0: await db_session.commit(); print(f"  Успешно суммаризировано {processed_posts_count} постов.")
            return f"Суммаризация завершена. Обработано: {processed_posts_count} постов."
        except Exception as e_async_sum: print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_summarizer: {e_async_sum}"); traceback.print_exc(); raise
        finally:
            if local_async_engine: await local_async_engine.dispose()
    try:
        result_message = asyncio.run(_async_main_logic_summarizer())
        task_duration = time.time() - task_start_time; print(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except Exception as e_task_sum: task_duration = time.time() - task_start_time; print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {e_task_sum}"); traceback.print_exc(); raise self.retry(exc=e_task_sum)

# --- ОБНОВЛЕННАЯ ЗАДАЧА ОТПРАВКИ ДАЙДЖЕСТА ---
@celery_instance.task(name="send_daily_digest", bind=True, max_retries=3, default_retry_delay=180)
def send_daily_digest_task(self, hours_ago_posts=24, top_n_metrics=3):
    task_start_time = time.time()
    print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Отправка РАСШИРЕННОГО ежедневного дайджеста)...")

    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_TARGET_CHAT_ID:
        error_msg = "Ошибка: TELEGRAM_BOT_TOKEN или TELEGRAM_TARGET_CHAT_ID не настроены."
        print(error_msg); return error_msg

    async def _async_send_digest_logic():
        bot = telegram.Bot(token=settings.TELEGRAM_BOT_TOKEN)
        ASYNC_DB_URL_FOR_TASK_DIGEST = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
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
                    # Уникальное имя для псевдонима Post в каждом вызове, убираем недопустимые символы
                    p_alias_name = f"p_digest_{''.join(filter(str.isalnum, metric_display_name.lower()))}"
                    p_alias = aliased(Post, name=p_alias_name)
                    
                    select_fields = [
                        p_alias.link, p_alias.summary_text, p_alias.post_sentiment_label,
                        Channel.title.label("channel_title")
                    ]
                    # Начинаем формировать запрос с базовыми полями
                    stmt_top = select(*select_fields)

                    order_by_col = None # Инициализируем

                    if is_sum_from_jsonb_flag:
                        # CTE для "разворачивания" массива реакций
                        rx_elements_cte_name = f"rx_elements_cte_{''.join(filter(str.isalnum, metric_display_name.lower()))}"
                        rx_elements_cte = (
                            select(
                                p_alias.id.label("post_id_for_reactions"),
                                cast(func.jsonb_array_elements(p_alias.reactions).op('->>')('count'), SAInteger).label("reaction_item_count")
                            )
                            .select_from(p_alias) 
                            .where(p_alias.reactions.isnot(None))
                            .where(func.jsonb_typeof(p_alias.reactions) == 'array')
                            .cte(rx_elements_cte_name)
                        )
                        # CTE для суммирования 'count' по каждому посту
                        sum_rx_cte_name = f"sum_rx_cte_{''.join(filter(str.isalnum, metric_display_name.lower()))}"
                        sum_reactions_cte = (
                            select(
                                rx_elements_cte.c.post_id_for_reactions.label("post_id"),
                                func.sum(rx_elements_cte.c.reaction_item_count).label("metric_value_for_digest")
                            )
                            .group_by(rx_elements_cte.c.post_id_for_reactions)
                            .cte(sum_rx_cte_name)
                        )
                        # Присоединяем CTE и добавляем вычисленную сумму в SELECT
                        stmt_top = stmt_top.add_columns(sum_reactions_cte.c.metric_value_for_digest)\
                                           .outerjoin(sum_reactions_cte, p_alias.id == sum_reactions_cte.c.post_id)
                        order_by_col = sum_reactions_cte.c.metric_value_for_digest
                    else:
                        # Для обычных колонок модели Post
                        actual_metric_column = getattr(p_alias, metric_column_or_expr_name)
                        stmt_top = stmt_top.add_columns(actual_metric_column.label("metric_value_for_digest"))
                        order_by_col = actual_metric_column
                    
                    # Общие JOINs и WHERE условия, применяем к p_alias
                    stmt_top = stmt_top.join(Channel, p_alias.channel_id == Channel.id)\
                                     .where(Channel.is_active == True)\
                                     .where(p_alias.posted_at >= time_threshold_posts)\
                                     .where(p_alias.summary_text.isnot(None))\
                                     .order_by(order_by_col.desc().nullslast())\
                                     .limit(top_n_metrics)
                    
                    for row in (await db_session.execute(stmt_top)).all():
                        top_posts_list.append({
                            "link": row.link, 
                            "summary": row.summary_text, 
                            "sentiment": row.post_sentiment_label, 
                            "channel_title": row.channel_title, 
                            "metric_value": row.metric_value_for_digest
                        })
                    return top_posts_list

                tops_to_include = [
                    {"metric_name": "comments_count", "label": "💬 Топ по комментариям", "emoji": "🗣️", "unit": "Комм."},
                    {"metric_name": "views_count", "label": "👀 Топ по просмотрам", "emoji": "👁️", "unit": "Просмотров"},
                    {"metric_name": "reactions", "label": "❤️ Топ по сумме реакций", "emoji": "👍", "unit": "Реакций", "is_sum_jsonb": True},
                ]

                for top_conf in tops_to_include:
                    posts_data = await get_top_posts_by_metric(
                        top_conf["metric_name"],
                        top_conf["label"], 
                        is_sum_from_jsonb_flag=top_conf.get("is_sum_jsonb", False)
                    )
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
            if len(digest_message_final) > 4096: 
                digest_message_final = digest_message_final[:4090] + helpers.escape_markdown("...", version=2)
                print("  ВНИМАНИЕ: Дайджест был обрезан из-за превышения лимита Telegram.")

            print(f"  Финальное сообщение дайджеста для Telegram:\n---\n{digest_message_final}\n---")
            await bot.send_message(chat_id=settings.TELEGRAM_TARGET_CHAT_ID, text=digest_message_final, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
            return f"Расширенный дайджест успешно отправлен."
        except Exception as e_digest_logic: print(f"!!! Ошибка в _async_send_digest_logic: {e_digest_logic}"); traceback.print_exc(); raise
        finally:
            if local_async_engine_digest: await local_async_engine_digest.dispose()
    try:
        result_message = asyncio.run(_async_send_digest_logic())
        task_duration = time.time() - task_start_time; print(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except Exception as e_task_digest: task_duration = time.time() - task_start_time; print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {e_task_digest}"); traceback.print_exc(); raise self.retry(exc=e_task_digest)

# --- ЗАДАЧА АНАЛИЗА ТОНАЛЬНОСТИ (AI) ---
@celery_instance.task(name="analyze_posts_sentiment", bind=True, max_retries=2, default_retry_delay=300)
def analyze_posts_sentiment_task(self, limit_posts_to_analyze=10):
    task_start_time = time.time(); print(f"Запущен Celery таск '{self.name}' (ID: {self.request.id}) (Анализ тональности, лимит: {limit_posts_to_analyze})...")
    if not settings.OPENAI_API_KEY: print("Ошибка: OPENAI_API_KEY не настроен."); return "Config error OpenAI"
    try: openai_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    except Exception as e_init_openai_sentiment: print(f"Ошибка OpenAI init: {e_init_openai_sentiment}"); raise self.retry(exc=e_init_openai_sentiment)
    ASYNC_DB_URL_FOR_TASK = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
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
                                         .order_by(Post.posted_at.asc()).limit(limit_posts_to_analyze))
                posts_to_process = (await db_session.execute(stmt_posts_to_analyze)).scalars().all()
                if not posts_to_process: print(f"  Не найдено постов для анализа тональности."); return "Нет постов для анализа."
                print(f"  Найдено {len(posts_to_process)} постов для анализа.")
                for post_obj in posts_to_process:
                    text_for_analysis = post_obj.caption_text if post_obj.caption_text else post_obj.text_content
                    if not text_for_analysis or not text_for_analysis.strip(): 
                        print(f"    Пост ID {post_obj.id} не имеет текста/подписи для анализа, пропускаем."); continue
                    print(f"    Анализ тональности поста ID {post_obj.id}...")
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
                                
                                if s_label not in ["positive","negative","neutral","mixed"]: 
                                    print(f"      ПРЕДУПРЕЖДЕНИЕ: LLM вернул невалидный sentiment_label '{s_label}'. Установлен 'neutral'.")
                                    s_label="neutral"
                                if not (-1.0 <= s_score <= 1.0): 
                                    print(f"      ПРЕДУПРЕЖДЕНИЕ: LLM вернул sentiment_score вне диапазона: {s_score}. Установлен 0.0.")
                                    s_score=0.0
                            except (json.JSONDecodeError, TypeError, ValueError) as e_json: 
                                print(f"  Ошибка парсинга JSON от LLM ({e_json}): {raw_resp}")
                        else:
                             print(f"      OpenAI вернул пустой ответ для поста ID {post_obj.id}.")
                            
                    except OpenAIError as e_llm_sentiment: 
                        print(f"    !!! Ошибка OpenAI API для поста ID {post_obj.id}: {e_llm_sentiment}"); 
                        continue 
                    except Exception as e_sa_general: 
                        print(f"    !!! Неожиданная ошибка анализа поста ID {post_obj.id}: {e_sa_general}"); 
                        traceback.print_exc(limit=1); 
                        continue 
                    
                    post_obj.post_sentiment_label = s_label
                    post_obj.post_sentiment_score = s_score
                    post_obj.updated_at = datetime.now(timezone.utc)
                    db_session.add(post_obj)
                    analyzed_posts_count += 1
                    print(f"      Тональность поста ID {post_obj.id}: {s_label} ({s_score:.2f})")

                if analyzed_posts_count > 0: await db_session.commit(); print(f"  Проанализировано {analyzed_posts_count} постов.")
            return f"Анализ тональности завершен. Обработано: {analyzed_posts_count} постов."
        except Exception as e_async_sa_main: print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в _async_main_logic_sentiment_analyzer: {e_async_sa_main}"); traceback.print_exc(); raise
        finally:
            if local_async_engine: await local_async_engine.dispose()
    try:
        result_message = asyncio.run(_async_main_logic_sentiment_analyzer())
        task_duration = time.time() - task_start_time; print(f"Celery таск '{self.name}' УСПЕШНО завершен за {task_duration:.2f} сек. Результат: {result_message}"); return result_message
    except Exception as e_task_sa_main: task_duration = time.time() - task_start_time; print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в таске '{self.name}' (за {task_duration:.2f} сек): {e_task_sa_main}"); traceback.print_exc(); raise self.retry(exc=e_task_sa_main)