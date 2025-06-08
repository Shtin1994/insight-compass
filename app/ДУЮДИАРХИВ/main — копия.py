# app/main.py

import asyncio
import logging
from datetime import datetime, timedelta, timezone, date 
from sqlalchemy import Date as SQLDate 
from typing import List, Optional, Dict, Tuple 
from enum import Enum as PyEnum 
import re 

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException, Query, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import IntegrityError 
import sqlalchemy as sa 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, asc, cast, literal_column, nullslast, update, delete, or_, text, Integer as SAInteger, Column as SAColumn
from sqlalchemy.orm import selectinload, aliased
from sqlalchemy.dialects.postgresql import JSONB 
from sqlalchemy.sql.expression import column

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import Channel as TelethonChannelType, Chat as TelethonChatType, User as TelethonUserType

from .models import Post, Comment, Channel 
from . import models as models_module
from .db.session import get_async_db 
from .celery_app import celery_instance
from .core.config import settings
from .schemas import ui_schemas

try:
    from .services.llm_service import одиночный_запрос_к_llm
except ImportError:
    async def одиночный_запрос_к_llm(prompt: str, модель: str, **kwargs) -> Optional[str]:
        logger.error("ЗАГЛУШКА: llm_service не найден или функция одиночный_запрос_к_llm отсутствует.")
        return "Заглушка: Ошибка вызова LLM сервиса."

# Импорт Celery задач
try:
    from app.tasks import ( # Сгруппировал импорты для читаемости
        collect_telegram_data_task, 
        summarize_top_posts_task, 
        send_daily_digest_task, 
        analyze_posts_sentiment_task, 
        enqueue_comments_for_ai_feature_analysis_task,
        advanced_data_refresh_task # <--- НОВЫЙ ИМПОРТ ЗАДАЧИ
    )
except ImportError as e:
    logging.getLogger(__name__).error(f"Ошибка импорта задач Celery: {e}")
    # Заглушки для всех задач, чтобы приложение могло запуститься
    def collect_telegram_data_task(*args, **kwargs): return type('obj', (object,), {'id': 'fake_task_id_collect'})()
    def summarize_top_posts_task(*args, **kwargs): return type('obj', (object,), {'id': 'fake_task_id_summarize'})()
    def send_daily_digest_task(*args, **kwargs): return type('obj', (object,), {'id': 'fake_task_id_digest'})()
    def analyze_posts_sentiment_task(*args, **kwargs): return type('obj', (object,), {'id': 'fake_task_id_sentiment'})()
    def enqueue_comments_for_ai_feature_analysis_task(*args, **kwargs): return type('obj', (object,), {'id': 'fake_task_id_comment_ai'})()
    def advanced_data_refresh_task(*args, **kwargs): return type('obj', (object,), {'id': 'fake_task_id_advanced_refresh'})()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) # Используем стандартный логгер для модуля
endpoint_logger = logging.getLogger("api_endpoints") # Отдельный логгер для эндпоинтов
endpoint_logger.setLevel(logging.INFO)

# Проверка и настройка обработчиков для endpoint_logger, чтобы избежать дублирования
if not endpoint_logger.handlers and not logging.getLogger().handlers: # Эта проверка может быть слишком строгой
    # Более простой вариант: всегда добавлять, если нет своих обработчиков
    if not endpoint_logger.hasHandlers():
        _handler = logging.StreamHandler()
        _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        _handler.setFormatter(_formatter)
        endpoint_logger.addHandler(_handler)
        endpoint_logger.propagate = False # Предотвращаем передачу логов корневому логгеру, если у нас свой обработчик

telegram_client_instance: Optional[TelegramClient] = None
async def get_telegram_client() -> TelegramClient:
    global telegram_client_instance
    if telegram_client_instance is None or not telegram_client_instance.is_connected():
        # Попытка переподключения, если клиент не None, но отсоединен
        if telegram_client_instance is not None and not telegram_client_instance.is_connected():
            logger.warning("Telegram client was disconnected. Attempting to reconnect for request...")
            try:
                await telegram_client_instance.connect()
                if not await telegram_client_instance.is_user_authorized():
                    logger.error("API: Reconnection attempt failed - user not authorized.")
                    raise HTTPException(status_code=503, detail="Telegram client not authorized after reconnect attempt.")
                logger.info("API: Successfully reconnected to Telegram for request.")
            except Exception as e:
                logger.error(f"API: Failed to reconnect Telegram client during request: {e}", exc_info=True)
                raise HTTPException(status_code=503, detail=f"Telegram client reconnection failed: {e}")
        else: # telegram_client_instance is None
            logger.error("Telegram client is None. Startup might have failed.")
            raise HTTPException(status_code=503, detail="Telegram client not available. Startup may have failed.")
    return telegram_client_instance

app = FastAPI(title=settings.PROJECT_NAME, version="0.1.0")
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], # В продакшене лучше указать конкретные домены
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)

@app.on_event("startup")
async def startup_event():
    global telegram_client_instance
    if not all([settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH, settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN]):
        logger.error("Telegram API credentials not configured. Telegram client will not be initialized.")
        telegram_client_instance = None # Явно устанавливаем в None
        return
    
    session_file_path = "/app/api_telegram_session" # Убедитесь, что эта папка существует и доступна для записи
    logger.info(f"API using session: {session_file_path}.session")
    
    # Инициализация клиента здесь
    telegram_client_instance = TelegramClient(session_file_path, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
    
    try:
        logger.info("API: Connecting to Telegram...")
        await telegram_client_instance.connect()
        if not await telegram_client_instance.is_user_authorized():
            logger.error(f"API: User NOT authorized for session {session_file_path}.session. Client initialized but not usable.")
            # Не устанавливаем telegram_client_instance в None, чтобы get_telegram_client мог попытаться переподключиться
        else:
            me = await telegram_client_instance.get_me()
            logger.info(f"API: Connected to Telegram as {me.first_name} (@{me.username or ''})")
    except Exception as e:
        logger.error(f"API: Failed to connect/authorize Telegram client during startup: {e}", exc_info=True)
        telegram_client_instance = None # В случае ошибки при запуске, делаем его None

@app.on_event("shutdown")
async def shutdown_event():
    global telegram_client_instance
    if telegram_client_instance and telegram_client_instance.is_connected():
        logger.info("Disconnecting Telegram client...")
        await telegram_client_instance.disconnect()
        logger.info("Telegram client disconnected.")
    elif telegram_client_instance:
        logger.info("Telegram client was initialized but not connected. No action needed for disconnection.")
    else:
        logger.info("Telegram client was not initialized. No action needed for disconnection.")


api_v1_router = APIRouter(prefix=settings.API_V1_STR)

@api_v1_router.post("/channels/", response_model=ui_schemas.ChannelResponse, status_code=201)
async def add_new_channel(channel_data: ui_schemas.ChannelCreateRequest, db: AsyncSession = Depends(get_async_db), tg_client: TelegramClient = Depends(get_telegram_client)):
    endpoint_logger.info(f"POST /channels/ - identifier: {channel_data.identifier}"); channel_identifier = channel_data.identifier.strip()
    try: entity = await tg_client.get_entity(channel_identifier)
    except (ValueError, ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError) as e: raise HTTPException(status_code=404, detail=f"TG entity '{channel_identifier}' not found/inaccessible: {e}")
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error with TG: {e}")
    if not isinstance(entity, TelethonChannelType):
        error_msg = f"Identifier '{channel_identifier}' is '{type(entity).__name__}', not channel/supergroup."
        if isinstance(entity, TelethonChatType): error_msg = f"Identifier '{channel_identifier}' is basic group, not supported."
        elif isinstance(entity, TelethonUserType): error_msg = f"Identifier '{channel_identifier}' is user, not channel."
        raise HTTPException(status_code=400, detail=error_msg)
    if not (getattr(entity, 'broadcast', False) or getattr(entity, 'megagroup', False)): raise HTTPException(status_code=400, detail=f"'{channel_identifier}' not broadcast channel or supergroup.")
    db_channel = (await db.execute(select(models_module.Channel).where(models_module.Channel.id == entity.id))).scalar_one_or_none()
    if db_channel:
        if not db_channel.is_active:
            db_channel.is_active = True; db_channel.title = entity.title; db_channel.username = getattr(entity, 'username', None); db_channel.description = getattr(entity, 'about', None)
            db.add(db_channel); await db.commit(); await db.refresh(db_channel); endpoint_logger.info(f"Channel ID {entity.id} ('{entity.title}') re-activated."); return db_channel
        else: endpoint_logger.info(f"Channel ID {entity.id} ('{entity.title}') already exists and is active."); raise HTTPException(status_code=409, detail=f"Channel '{entity.title}' (ID: {entity.id}) already tracked.")
    new_channel_model = models_module.Channel(id=entity.id, username=getattr(entity, 'username', None), title=entity.title, description=getattr(entity, 'about', None), is_active=True)
    db.add(new_channel_model)
    try: await db.commit(); await db.refresh(new_channel_model); endpoint_logger.info(f"Channel '{new_channel_model.title}' (ID: {new_channel_model.id}) added to database.")
    except IntegrityError: await db.rollback(); endpoint_logger.warning(f"IntegrityError for channel ID {entity.id}. Race condition?"); raise HTTPException(status_code=409, detail=f"Channel '{entity.title}' (ID: {entity.id}) was added by another process or DB conflict.")
    return new_channel_model

@api_v1_router.get("/channels/", response_model=ui_schemas.PaginatedChannelsResponse)
async def get_tracked_channels(skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=100), db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /channels/ - skip={skip}, limit={limit}")
    total_stmt = select(func.count(models_module.Channel.id))
    channels_stmt = select(models_module.Channel).order_by(models_module.Channel.title).offset(skip).limit(limit)
    total_channels = (await db.execute(total_stmt)).scalar_one_or_none() or 0
    channels_list = (await db.execute(channels_stmt)).scalars().all()
    return ui_schemas.PaginatedChannelsResponse(total_channels=total_channels, channels=channels_list)

@api_v1_router.get("/channels/{channel_id}/", response_model=ui_schemas.ChannelResponse)
async def get_channel_details(channel_id: int, db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /channels/{channel_id}/")
    channel = await db.get(models_module.Channel, channel_id)
    if not channel: raise HTTPException(status_code=404, detail="Channel not found")
    return channel

@api_v1_router.put("/channels/{channel_id}/", response_model=ui_schemas.ChannelResponse)
async def update_channel_status(channel_id: int, channel_update_data: ui_schemas.ChannelUpdateRequest, db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"PUT /channels/{channel_id}/ - data: {channel_update_data.model_dump(exclude_unset=True)}")
    channel = await db.get(models_module.Channel, channel_id)
    if not channel: raise HTTPException(status_code=404, detail="Channel not found")
    update_data = channel_update_data.model_dump(exclude_unset=True)
    if not update_data: raise HTTPException(status_code=400, detail="No update data provided.")
    for field, value in update_data.items(): setattr(channel, field, value)
    db.add(channel); await db.commit(); await db.refresh(channel); endpoint_logger.info(f"Channel ID {channel_id} updated: {update_data}")
    return channel

@api_v1_router.delete("/channels/{channel_id}/", status_code=204)
async def delete_tracked_channel(channel_id: int, db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"DELETE /channels/{channel_id}/")
    channel = await db.get(models_module.Channel, channel_id)
    if not channel: raise HTTPException(status_code=404, detail="Channel not found")
    if channel.is_active: channel.is_active = False; db.add(channel); await db.commit(); endpoint_logger.info(f"Channel ID {channel_id} ('{channel.title}') deactivated.")
    else: endpoint_logger.info(f"Channel ID {channel_id} ('{channel.title}') already inactive.")
    return None

@api_v1_router.get("/dashboard/stats", response_model=ui_schemas.DashboardStatsResponse)
async def get_dashboard_stats(db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info("GET /api/v1/dashboard/stats")
    try:
        active_channels_subquery = select(models_module.Channel.id).where(models_module.Channel.is_active == True).subquery()
        total_posts_stmt = select(func.count(models_module.Post.id)).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id)
        total_posts_all_time = (await db.execute(total_posts_stmt)).scalar_one_or_none() or 0
        total_comments_stmt = (select(func.count(models_module.Comment.id)).join(models_module.Post, models_module.Comment.post_id == models_module.Post.id).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id))
        total_comments_all_time = (await db.execute(total_comments_stmt)).scalar_one_or_none() or 0
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        posts_last_7_stmt = (select(func.count(models_module.Post.id)).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(models_module.Post.posted_at >= seven_days_ago))
        posts_last_7_days = (await db.execute(posts_last_7_stmt)).scalar_one_or_none() or 0
        comments_last_7_stmt = (select(func.count(models_module.Comment.id)).join(models_module.Post, models_module.Comment.post_id == models_module.Post.id).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(models_module.Comment.commented_at >= seven_days_ago))
        comments_last_7_days = (await db.execute(comments_last_7_stmt)).scalar_one_or_none() or 0
        channels_count_stmt = select(func.count(models_module.Channel.id)).where(models_module.Channel.is_active == True)
        channels_monitoring_count = (await db.execute(channels_count_stmt)).scalar_one_or_none() or 0
        return ui_schemas.DashboardStatsResponse(total_posts_all_time=total_posts_all_time, total_comments_all_time=total_comments_all_time, posts_last_7_days=posts_last_7_days, comments_last_7_days=comments_last_7_days, channels_monitoring_count=channels_monitoring_count)
    except Exception as e: endpoint_logger.error(f"Error in get_dashboard_stats: {e}", exc_info=True); raise HTTPException(status_code=500, detail="Internal server error while fetching dashboard stats")

@api_v1_router.get("/dashboard/activity_over_time", response_model=ui_schemas.ActivityOverTimeResponse)
async def get_activity_over_time(days: int = Query(7, ge=1, le=90), db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /api/v1/dashboard/activity_over_time?days={days}")
    try:
        start_date_dt = datetime.now(timezone.utc) - timedelta(days=days -1); start_date = start_date_dt.date()
        active_channels_subquery = select(models_module.Channel.id).where(models_module.Channel.is_active == True).subquery()
        posts_activity_stmt = (select(cast(models_module.Post.posted_at, SQLDate).label("activity_day"), func.count(models_module.Post.id).label("count")).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(cast(models_module.Post.posted_at, SQLDate) >= start_date).group_by(cast(models_module.Post.posted_at, SQLDate)).order_by(cast(models_module.Post.posted_at, SQLDate).asc()))
        posts_by_day_result = await db.execute(posts_activity_stmt)
        posts_by_day = {row.activity_day: row.count for row in posts_by_day_result.all()}
        comments_activity_stmt = (select(cast(models_module.Comment.commented_at, SQLDate).label("activity_day"), func.count(models_module.Comment.id).label("count")).join(models_module.Post, models_module.Comment.post_id == models_module.Post.id).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(cast(models_module.Comment.commented_at, SQLDate) >= start_date).group_by(cast(models_module.Comment.commented_at, SQLDate)).order_by(cast(models_module.Comment.commented_at, SQLDate).asc()))
        comments_by_day_result = await db.execute(comments_activity_stmt)
        comments_by_day = {row.activity_day: row.count for row in comments_by_day_result.all()}
        activity_data: List[ui_schemas.ActivityOverTimePoint] = []
        current_date_iter = start_date; end_date = datetime.now(timezone.utc).date()
        while current_date_iter <= end_date:
            activity_data.append(ui_schemas.ActivityOverTimePoint(activity_date=current_date_iter, post_count=posts_by_day.get(current_date_iter, 0), comment_count=comments_by_day.get(current_date_iter, 0)))
            current_date_iter += timedelta(days=1)
        return ui_schemas.ActivityOverTimeResponse(data=activity_data)
    except Exception as e: endpoint_logger.error(f"Error in get_activity_over_time: {e}", exc_info=True); raise HTTPException(status_code=500, detail="Internal server error while fetching activity over time")

@api_v1_router.get("/dashboard/top_channels", response_model=ui_schemas.TopChannelsResponse)
async def get_top_channels(metric: str = Query("posts", pattern="^(posts|comments)$"), limit: int = Query(5, ge=1, le=20), days_period: int = Query(7, ge=1, le=365), db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /api/v1/dashboard/top_channels?metric={metric}&limit={limit}&days_period={days_period}")
    try:
        start_date = datetime.now(timezone.utc) - timedelta(days=days_period)
        base_query_channels = select(models_module.Channel.id.label("channel_id"), models_module.Channel.title.label("channel_title"), models_module.Channel.username.label("channel_username")).where(models_module.Channel.is_active == True)
        if metric == "posts":
            stmt = (base_query_channels.add_columns(func.count(models_module.Post.id).label("metric_value")).join(models_module.Post, models_module.Channel.id == models_module.Post.channel_id).where(models_module.Post.posted_at >= start_date).group_by(models_module.Channel.id, models_module.Channel.title, models_module.Channel.username).order_by(desc(literal_column("metric_value"))).limit(limit))
        elif metric == "comments":
            stmt = (base_query_channels.add_columns(func.count(models_module.Comment.id).label("metric_value")).join(models_module.Post, models_module.Channel.id == models_module.Post.channel_id).join(models_module.Comment, models_module.Post.id == models_module.Comment.post_id).where(models_module.Comment.commented_at >= start_date).group_by(models_module.Channel.id, models_module.Channel.title, models_module.Channel.username).order_by(desc(literal_column("metric_value"))).limit(limit))
        else: raise HTTPException(status_code=400, detail="Invalid metric type specified.")
        top_channels_data_result = await db.execute(stmt)
        top_channels_data = top_channels_data_result.all()
        data_list = [ui_schemas.TopChannelItem(channel_id=row.channel_id, channel_title=row.channel_title, channel_username=row.channel_username, metric_value=row.metric_value) for row in top_channels_data]
        return ui_schemas.TopChannelsResponse(metric_name=metric, data=data_list)
    except Exception as e: endpoint_logger.error(f"Error in get_top_channels: {e}", exc_info=True); raise HTTPException(status_code=500, detail="Internal server error while fetching top channels")

@api_v1_router.get("/dashboard/sentiment_distribution", response_model=ui_schemas.SentimentDistributionResponse)
async def get_sentiment_distribution(days_period: int = Query(7, ge=1, le=365), db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /api/v1/dashboard/sentiment_distribution?days_period={days_period}")
    try:
        start_date = datetime.now(timezone.utc) - timedelta(days=days_period)
        active_channels_subquery = select(models_module.Channel.id).where(models_module.Channel.is_active == True).subquery()
        total_analyzed_stmt = (select(func.count(models_module.Post.id)).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(models_module.Post.posted_at >= start_date).where(models_module.Post.post_sentiment_label.isnot(None)))
        total_analyzed_posts = (await db.execute(total_analyzed_stmt)).scalar_one_or_none() or 0
        stmt = (select(models_module.Post.post_sentiment_label.label("sentiment_label"), func.count(models_module.Post.id).label("count")).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(models_module.Post.posted_at >= start_date).group_by(models_module.Post.post_sentiment_label).order_by(nullslast(models_module.Post.post_sentiment_label.asc())))
        sentiment_counts_result = await db.execute(stmt)
        sentiment_counts = sentiment_counts_result.all()
        data_list: List[ui_schemas.SentimentDistributionItem] = []
        defined_labels = ["positive", "negative", "neutral", "mixed"]; counts_map = {row.sentiment_label: row.count for row in sentiment_counts if row.sentiment_label is not None}; undefined_count = next((row.count for row in sentiment_counts if row.sentiment_label is None), 0)
        for label in defined_labels:
            count = counts_map.get(label, 0); percentage = round((count / total_analyzed_posts) * 100, 2) if total_analyzed_posts > 0 else 0.0
            data_list.append(ui_schemas.SentimentDistributionItem(sentiment_label=label, count=count, percentage=percentage))
        if undefined_count > 0:
            all_posts_in_period_stmt = select(func.count(models_module.Post.id)).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(models_module.Post.posted_at >= start_date)
            all_posts_in_period = (await db.execute(all_posts_in_period_stmt)).scalar_one_or_none() or 0
            undefined_percentage = round((undefined_count / all_posts_in_period) * 100, 2) if all_posts_in_period > 0 else 0.0
            data_list.append(ui_schemas.SentimentDistributionItem(sentiment_label="undefined", count=undefined_count, percentage=undefined_percentage))
        return ui_schemas.SentimentDistributionResponse(total_analyzed_posts=total_analyzed_posts, data=data_list)
    except Exception as e: endpoint_logger.error(f"Error in get_sentiment_distribution: {e}", exc_info=True); raise HTTPException(status_code=500, detail="Internal server error while fetching sentiment distribution")

@api_v1_router.get("/dashboard/comment_insights", response_model=ui_schemas.CommentInsightsResponse)
async def get_comment_insights(
    days_period: int = Query(7, ge=1, le=365, description="Период в днях для анализа"),
    top_n: int = Query(10, ge=1, le=50, description="Количество топовых элементов для каждой категории"),
    db: AsyncSession = Depends(get_async_db)
):
    endpoint_logger.info(f"GET /api/v1/dashboard/comment_insights?days_period={days_period}&top_n={top_n}")
    start_date = datetime.now(timezone.utc) - timedelta(days=days_period)
    active_channels_subquery = select(models_module.Channel.id).where(models_module.Channel.is_active == True).subquery("active_channels_for_insights")
    async def fetch_top_jsonb_array_elements(jsonb_field_name: str) -> List[ui_schemas.InsightItem]:
        comment_jsonb_field = getattr(models_module.Comment, jsonb_field_name)
        item_text_expression = func.jsonb_array_elements_text(comment_jsonb_field).label("item_text")
        stmt = (
            select(item_text_expression, func.count().label("item_count"))
            .select_from(models_module.Comment)
            .join(models_module.Post, models_module.Comment.post_id == models_module.Post.id)
            .join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id)
            .where(models_module.Comment.commented_at >= start_date)
            .where(comment_jsonb_field.isnot(None)) 
            .where(func.jsonb_typeof(comment_jsonb_field) == 'array')
            .group_by(item_text_expression)
            .order_by(desc(literal_column("item_count")), literal_column("item_text").asc()) 
            .limit(top_n)
        )
        results = await db.execute(stmt)
        return [ui_schemas.InsightItem(text=str(row.item_text), count=row.item_count) for row in results.all()]
    try:
        top_topics_data = await fetch_top_jsonb_array_elements("extracted_topics")
        top_problems_data = await fetch_top_jsonb_array_elements("extracted_problems")
        top_questions_data = await fetch_top_jsonb_array_elements("extracted_questions")
        top_suggestions_data = await fetch_top_jsonb_array_elements("extracted_suggestions")
        return ui_schemas.CommentInsightsResponse(
            period_days=days_period,
            top_topics=top_topics_data,
            top_problems=top_problems_data,
            top_questions=top_questions_data,
            top_suggestions=top_suggestions_data
        )
    except Exception as e:
        endpoint_logger.error(f"Error in get_comment_insights: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching comment insights")

@api_v1_router.get("/dashboard/insight_item_trend", response_model=ui_schemas.InsightItemTrendResponse)
async def get_insight_item_trend(
    item_type: ui_schemas.InsightItemType = Query(..., description="Тип инсайта: topic, problem, question, suggestion"),
    item_text: str = Query(..., min_length=1, max_length=200, description="Текст искомого инсайта"),
    days_period: int = Query(30, ge=1, le=365, description="Период в днях для анализа"),
    granularity: ui_schemas.TrendGranularity = Query(ui_schemas.TrendGranularity.DAY, description="Гранулярность: day или week"),
    db: AsyncSession = Depends(get_async_db)
):
    endpoint_logger.info(f"GET /dashboard/insight_item_trend - item_type={item_type.value}, item_text='{item_text}', days_period={days_period}, granularity={granularity.value}")
    today = datetime.now(timezone.utc).date()
    query_start_date = today - timedelta(days=days_period -1)
    jsonb_field_map = {
        ui_schemas.InsightItemType.TOPIC: models_module.Comment.extracted_topics,
        ui_schemas.InsightItemType.PROBLEM: models_module.Comment.extracted_problems,
        ui_schemas.InsightItemType.QUESTION: models_module.Comment.extracted_questions,
        ui_schemas.InsightItemType.SUGGESTION: models_module.Comment.extracted_suggestions,
    }
    comment_jsonb_field = jsonb_field_map.get(item_type)
    if comment_jsonb_field is None: raise HTTPException(status_code=400, detail="Invalid item_type specified.")
    active_channels_subquery = select(models_module.Channel.id).where(models_module.Channel.is_active == True).subquery("active_channels_for_trend")
    date_group_expression_col: SAColumn; date_format_str: str
    if granularity == ui_schemas.TrendGranularity.DAY:
        date_group_expression_col = cast(models_module.Comment.commented_at, SQLDate).label("trend_date_label")
        date_format_str = "%Y-%m-%d"
    elif granularity == ui_schemas.TrendGranularity.WEEK:
        date_group_expression_col = func.to_char(models_module.Comment.commented_at, 'YYYY-WW').label("trend_date_label") # Используем 'YYYY-WW' для недели
        date_format_str = "YYYY-WW" # Соответствующий формат для ключа
    else: raise HTTPException(status_code=400, detail="Invalid granularity specified.")
    
    extracted_element_alias = func.jsonb_array_elements_text(comment_jsonb_field).alias("extracted_insight_element")
    stmt = (
        select(date_group_expression_col, func.count(models_module.Comment.id).label("item_count"))
        .select_from(models_module.Comment)
        .join(models_module.Post, models_module.Comment.post_id == models_module.Post.id)
        .join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id)
        .join(extracted_element_alias, sa.true()) 
        .where(cast(models_module.Comment.commented_at, SQLDate) >= query_start_date) 
        .where(comment_jsonb_field.isnot(None)).where(func.jsonb_typeof(comment_jsonb_field) == 'array')
        .where(column("extracted_insight_element").ilike(f"%{item_text}%")) 
        .group_by(date_group_expression_col).order_by(date_group_expression_col.asc()) 
    )
    results = await db.execute(stmt)
    existing_data_map: Dict[str, int] = {str(row.trend_date_label): row.item_count for row in results.all()}
    trend_data_points: List[ui_schemas.InsightTrendDataPoint] = []
    
    if granularity == ui_schemas.TrendGranularity.DAY:
        current_iter_date = query_start_date
        while current_iter_date <= today:
            date_key = current_iter_date.strftime(date_format_str)
            trend_data_points.append(ui_schemas.InsightTrendDataPoint(date=date_key, count=existing_data_map.get(date_key, 0)))
            current_iter_date += timedelta(days=1)
    elif granularity == ui_schemas.TrendGranularity.WEEK:
        all_week_keys_in_period = set()
        temp_date = query_start_date
        while temp_date <= today: # Итерируемся по дням, чтобы получить все недели в периоде
            all_week_keys_in_period.add(temp_date.strftime(date_format_str)) # PostgreSQL to_char 'YYYY-WW'
            temp_date += timedelta(days=1) 
        for week_key in sorted(list(all_week_keys_in_period)): # Сортируем недели
            trend_data_points.append(ui_schemas.InsightTrendDataPoint(date=week_key, count=existing_data_map.get(week_key,0)))
            
    return ui_schemas.InsightItemTrendResponse(item_type=item_type, item_text=item_text, period_days=days_period, granularity=granularity, trend_data=trend_data_points)

# --- ЭНДПОИНТ ДЛЯ ВОПРОСОВ НА ЕСТЕСТВЕННОМ ЯЗЫКЕ ---
@api_v1_router.post("/natural_language_query/", response_model=ui_schemas.NLQueryResponse)
async def natural_language_query(
    request_data: ui_schemas.NLQueryRequest, 
    db: AsyncSession = Depends(get_async_db)
):
    query_text_lower = request_data.query_text.lower()
    endpoint_logger.info(f"POST /natural_language_query/ - query: '{request_data.query_text}', explicit_days_period: {request_data.days_period}")

    days_period_nlq = request_data.days_period 

    if days_period_nlq is None: 
        match_days = re.search(r"за (?:последние |)\s*(\d+)\s*(день|дней|дня|недел)", query_text_lower)
        if match_days:
            num = int(match_days.group(1))
            unit = match_days.group(2)
            if "недел" in unit: days_period_nlq = num * 7
            else: days_period_nlq = num
            endpoint_logger.info(f"Извлечен период из текста: {days_period_nlq} дней.")
        else:
            days_period_nlq = 7 
            endpoint_logger.info(f"Период не найден в тексте, используется по умолчанию: {days_period_nlq} дней.")
    else:
        endpoint_logger.info(f"Используется явно переданный период: {days_period_nlq} дней.")

    insight_type_nlq: Optional[ui_schemas.InsightItemType] = None
    jsonb_field_nlq: Optional[str] = None
    insight_name_for_prompt = "информацию"
    insight_name_plural_for_prompt = "информации"

    if "тем" in query_text_lower:
        insight_type_nlq = ui_schemas.InsightItemType.TOPIC
        jsonb_field_nlq = "extracted_topics"
        insight_name_for_prompt = "основные темы"
        insight_name_plural_for_prompt = "основных тем"
    elif "проблем" in query_text_lower:
        insight_type_nlq = ui_schemas.InsightItemType.PROBLEM
        jsonb_field_nlq = "extracted_problems"
        insight_name_for_prompt = "основные проблемы"
        insight_name_plural_for_prompt = "основных проблем"
    elif "вопрос" in query_text_lower: 
        insight_type_nlq = ui_schemas.InsightItemType.QUESTION
        jsonb_field_nlq = "extracted_questions"
        insight_name_for_prompt = "основные вопросы"
        insight_name_plural_for_prompt = "основных вопросов"
    elif "предложен" in query_text_lower: 
        insight_type_nlq = ui_schemas.InsightItemType.SUGGESTION
        jsonb_field_nlq = "extracted_suggestions"
        insight_name_for_prompt = "основные предложения"
        insight_name_plural_for_prompt = "основных предложений"

    ai_answer: str
    
    if insight_type_nlq and jsonb_field_nlq:
        try:
            start_date_nlq = datetime.now(timezone.utc) - timedelta(days=days_period_nlq)
            active_channels_subquery_nlq = select(models_module.Channel.id).where(models_module.Channel.is_active == True).subquery()
            comment_jsonb_field_model = getattr(models_module.Comment, jsonb_field_nlq)
            item_text_expr_nlq = func.jsonb_array_elements_text(comment_jsonb_field_model).label("item_text")
            stmt_nlq = (
                select(item_text_expr_nlq, func.count().label("item_count"))
                .select_from(models_module.Comment)
                .join(models_module.Post, models_module.Comment.post_id == models_module.Post.id)
                .join(active_channels_subquery_nlq, models_module.Post.channel_id == active_channels_subquery_nlq.c.id)
                .where(models_module.Comment.commented_at >= start_date_nlq)
                .where(comment_jsonb_field_model.isnot(None)) 
                .where(func.jsonb_typeof(comment_jsonb_field_model) == 'array')
                .group_by(item_text_expr_nlq)
                .order_by(desc(literal_column("item_count")))
                .limit(5) 
            )
            results_nlq = await db.execute(stmt_nlq)
            top_insights_data = [{"text": str(row.item_text), "count": row.item_count} for row in results_nlq.all()]

            if not top_insights_data:
                ai_answer = f"За последние {days_period_nlq} дней не найдено информации по запрошенному типу '{insight_name_plural_for_prompt}'."
            else:
                context_for_llm = f"Данные о топ-{len(top_insights_data)} {insight_name_for_prompt.replace('ые', 'ых')} за последние {days_period_nlq} дней:\n"
                for item in top_insights_data:
                    context_for_llm += f"- \"{item['text']}\" (упоминаний: {item['count']})\n"
                
                prompt_for_llm = (
                    f"Ты — AI-аналитик данных из Telegram-каналов. "
                    f"Твоя задача — ответить на вопрос пользователя, используя предоставленные данные. "
                    f"Предоставленные данные актуальны за период: последние {days_period_nlq} дней. "
                    f"Отвечай кратко и по существу, основываясь ИСКЛЮЧИТЕЛЬНО на этих данных и указанном периоде ({days_period_nlq} дней).\n\n"
                    f"ПРЕДОСТАВЛЕННЫЕ ДАННЫЕ (для ответа на вопрос):\n{context_for_llm}\n"
                    f"ВОПРОС ПОЛЬЗОВАТЕЛЯ: \"{request_data.query_text}\"\n\n"
                    f"ТВОЙ ОТВЕТ (сформулируй его как естественный текстовый ответ, упоминая, что информация относится к последним {days_period_nlq} дням, если это не противоречит сути вопроса; не используй период из текста вопроса пользователя, если он отличается от {days_period_nlq} дней; не перечисляй просто данные, а дай связный ответ):"
                )
                endpoint_logger.info(f"Промпт для LLM (NLQ):\n{prompt_for_llm}")
                
                llm_response = await одиночный_запрос_к_llm(
                    prompt_for_llm, 
                    модель=settings.OPENAI_DEFAULT_MODEL_FOR_TASKS or "gpt-3.5-turbo-1106",
                    is_json_response_expected=False 
                )
                ai_answer = llm_response or "Не удалось получить ответ от AI."
        except Exception as e_nlq_data:
            endpoint_logger.error(f"Ошибка при подготовке данных или вызове LLM для NLQ: {e_nlq_data}", exc_info=True)
            ai_answer = "Произошла внутренняя ошибка при обработке вашего запроса. Не удалось получить данные или связаться с AI."
    else:
        ai_answer = "Извините, я пока не могу ответить на такой тип вопроса. Попробуйте спросить про основные темы, проблемы, вопросы или предложения за определенный период (например, 'Какие темы обсуждали за 7 дней?')."

    return ui_schemas.NLQueryResponse(
        original_query=request_data.query_text, 
        ai_answer=ai_answer
    )

# --- ENUMs для сортировки ---
class PostSortByField(PyEnum): 
    posted_at = "posted_at"; comments_count = "comments_count"; views_count = "views_count"; forwards_count = "forwards_count"; reactions_total_sum = "reactions_total_sum"

# --- ФУНКЦИЯ get_comment_author_display_name ---
def get_comment_author_display_name(comment: models_module.Comment) -> str:
    if hasattr(comment, 'user_fullname') and comment.user_fullname and comment.user_fullname.strip(): return comment.user_fullname.strip()
    if hasattr(comment, 'user_username') and comment.user_username: return f"@{comment.user_username}"
    if hasattr(comment, 'telegram_user_id') and comment.telegram_user_id: return f"User_{str(comment.telegram_user_id)}"
    return "Unknown Author"

# --- ОБНОВЛЕННЫЙ ЭНДПОИНТ ДЛЯ ПОЛУЧЕНИЯ ПОСТОВ С СОРТИРОВКОЙ И ПОИСКОМ ---
@api_v1_router.get("/posts/", response_model=ui_schemas.PaginatedPostsResponse)
async def get_posts_for_ui(
    page: int = Query(1, ge=1, description="Page number"), 
    limit: int = Query(10, ge=1, le=100, description="Number of items per page"),
    search_query: Optional[str] = Query(None),
    sort_by: PostSortByField = Query(PostSortByField.posted_at.value, description="Поле для сортировки"), 
    sort_order: str = Query("desc", pattern="^(asc|desc)$", description="Порядок сортировки"),
    db: AsyncSession = Depends(get_async_db)
):
    skip = (page - 1) * limit 
    endpoint_logger.info(f"GET /api/v1/posts/ - page={page} (skip={skip}), limit={limit}, search_query='{search_query}', sort_by='{sort_by.value}', sort_order='{sort_order}'")
    try:
        CurrentPostModel = models_module.Post; CurrentChannelModel = models_module.Channel
        p_alias = aliased(CurrentPostModel, name="p_for_reactions_cte")
        reaction_elements_cte_subquery = (select(p_alias.id.label("post_id_in_cte"), cast(func.jsonb_array_elements(p_alias.reactions).op('->>')('count'), SAInteger).label("reaction_item_count")).select_from(p_alias).where(p_alias.reactions.isnot(None)).where(func.jsonb_typeof(p_alias.reactions) == 'array').cte("reaction_elements_cte"))
        sum_of_reactions_cte = (select(reaction_elements_cte_subquery.c.post_id_in_cte.label("post_id"), func.sum(reaction_elements_cte_subquery.c.reaction_item_count).label("total_reactions_sum")).group_by(reaction_elements_cte_subquery.c.post_id_in_cte).cte("sum_of_reactions_cte"))
        posts_select_stmt = select(CurrentPostModel)
        if sort_by == PostSortByField.reactions_total_sum: posts_select_stmt = posts_select_stmt.outerjoin(sum_of_reactions_cte, CurrentPostModel.id == sum_of_reactions_cte.c.post_id).add_columns(sum_of_reactions_cte.c.total_reactions_sum.label("calculated_total_reactions"))
        posts_select_stmt = posts_select_stmt.join(CurrentChannelModel, CurrentPostModel.channel_id == CurrentChannelModel.id)
        conditions = [CurrentChannelModel.is_active == True]
        if search_query: search_pattern = f"%{search_query}%"; search_conditions_list = [CurrentPostModel.text_content.ilike(search_pattern), CurrentPostModel.caption_text.ilike(search_pattern), CurrentPostModel.summary_text.ilike(search_pattern)]; conditions.append(or_(*[cond for cond in search_conditions_list if cond is not None]))
        posts_with_conditions_stmt = posts_select_stmt.where(*conditions)
        count_query = select(func.count()).select_from(posts_with_conditions_stmt.with_only_columns(CurrentPostModel.id).alias("sub_count_query"))
        total_posts_result = await db.execute(count_query); total_posts = total_posts_result.scalar_one_or_none() or 0
        order_expression = None
        if sort_by == PostSortByField.reactions_total_sum: order_field = literal_column("calculated_total_reactions"); order_expression = asc(order_field).nullsfirst() if sort_order == "asc" else desc(order_field).nullslast() 
        else:
            order_field_attr = getattr(CurrentPostModel, sort_by.value, CurrentPostModel.posted_at)
            if sort_by in [PostSortByField.views_count, PostSortByField.forwards_count, PostSortByField.comments_count]: order_expression = asc(order_field_attr).nullsfirst() if sort_order == "asc" else desc(order_field_attr).nullslast()
            else: order_expression = asc(order_field_attr) if sort_order == "asc" else desc(order_field_attr)
        final_posts_query = posts_with_conditions_stmt.order_by(order_expression).options(selectinload(CurrentPostModel.channel)).offset(skip).limit(limit)
        results = await db.execute(final_posts_query)
        if sort_by == PostSortByField.reactions_total_sum: posts_scalars = [row[0] for row in results.all()] 
        else: posts_scalars = results.scalars().unique().all() 
        posts_list = [ui_schemas.PostListItem.model_validate(post) for post in posts_scalars]
        return ui_schemas.PaginatedPostsResponse(total_posts=total_posts, posts=posts_list)
    except Exception as e: endpoint_logger.error(f"Error in get_posts_for_ui: {e}", exc_info=True); 
    if hasattr(e, 'errors') and callable(e.errors): endpoint_logger.error(f"Pydantic ValidationError details: {e.errors()}")
    raise HTTPException(status_code=500, detail="Internal server error while fetching posts")

@api_v1_router.get("/posts/{post_id}/comments/", response_model=ui_schemas.PaginatedCommentsResponse)
async def get_comments_for_post_ui(post_id: int, skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=100), db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /api/v1/posts/{post_id}/comments/ - skip={skip}, limit={limit}")
    try:
        CurrentPostModel = models_module.Post; CurrentCommentModel = models_module.Comment
        post_check_stmt = (select(CurrentPostModel).join(models_module.Channel, CurrentPostModel.channel_id == models_module.Channel.id).where(CurrentPostModel.id == post_id).where(models_module.Channel.is_active == True))
        post = (await db.execute(post_check_stmt)).scalar_one_or_none()
        if not post: raise HTTPException(status_code=404, detail=f"Post with ID {post_id} not found or not accessible.")
        total_comments_stmt = select(func.count(CurrentCommentModel.id)).where(CurrentCommentModel.post_id == post_id)
        total_comments = (await db.execute(total_comments_stmt)).scalar_one_or_none() or 0
        comments_stmt = (select(CurrentCommentModel).where(CurrentCommentModel.post_id == post_id).order_by(asc(CurrentCommentModel.commented_at)).offset(skip).limit(limit))
        comments_scalars = (await db.execute(comments_stmt)).scalars().all()
        comments_list = [ui_schemas.CommentListItem(id=c.id, author_display_name=get_comment_author_display_name(c), text=c.text_content, commented_at=c.commented_at) for c in comments_scalars]
        return ui_schemas.PaginatedCommentsResponse(total_comments=total_comments, comments=comments_list)
    except Exception as e:
        endpoint_logger.error(f"Error in get_comments_for_post_ui (post_id={post_id}): {e}", exc_info=True)
        if hasattr(e, 'errors') and callable(e.errors): endpoint_logger.error(f"Pydantic ValidationError details: {e.errors()}")
        raise HTTPException(status_code=500, detail=f"Internal server error while fetching comments for post {post_id}")

# --- Эндпоинты для запуска Celery задач ---
@api_v1_router.post("/run-collection-task/", summary="Запустить задачу сбора данных")
async def run_collection_task_endpoint(): 
    task = collect_telegram_data_task.delay()
    return {"message": "Задача сбора данных запущена.", "task_id": task.id}

@api_v1_router.post("/run-summarization-task/", summary="Запустить задачу суммаризации")
async def run_summarization_task_endpoint(): 
    task = summarize_top_posts_task.delay()
    return {"message": "Задача суммаризации запущена.", "task_id": task.id}

@api_v1_router.post("/run-daily-digest-task/", summary="Запустить задачу дайджеста")
async def run_daily_digest_task_endpoint(): 
    task = send_daily_digest_task.delay()
    return {"message": "Задача дайджеста запущена.", "task_id": task.id}

@api_v1_router.post("/run-sentiment-analysis-task/", summary="Запустить задачу анализа тональности")
async def run_sentiment_analysis_task_endpoint(): 
    task = analyze_posts_sentiment_task.delay(limit_posts_to_analyze=settings.POST_FETCH_LIMIT or 5)
    return {"message": "Задача анализа тональности запущена.", "task_id": task.id}

@api_v1_router.post("/run-comment-feature-analysis/", summary="Запустить AI-анализ фич комментариев")
async def run_comment_feature_analysis_endpoint(limit: int = Query(100, ge=1, le=1000, description="Количество комментариев для постановки в очередь на анализ")): # Увеличил дефолт и макс лимит
    endpoint_logger.info(f"POST /run-comment-feature-analysis/ - limit={limit}")
    task = enqueue_comments_for_ai_feature_analysis_task.delay(limit_comments_to_queue=limit)
    return {"message": f"Задача AI-анализа фич для ~{limit} комментариев запущена.", "task_id": task.id}

# --- НОВЫЙ ЭНДПОИНТ ДЛЯ ПРОДВИНУТОГО ОБНОВЛЕНИЯ ДАННЫХ ---
@api_v1_router.post(
    "/run-advanced-data-refresh/", 
    response_model=ui_schemas.AdvancedDataRefreshResponse,
    summary="Запустить задачу продвинутого обновления данных",
    description="Запускает комплексную задачу сбора и обновления данных постов и комментариев с различными параметрами, с последующим AI-анализом новых комментариев."
)
async def run_advanced_data_refresh_endpoint(
    refresh_request: ui_schemas.AdvancedDataRefreshRequest,
    # BackgroundTasks здесь не нужен, так как задача Celery асинхронна сама по себе
):
    endpoint_logger.info(f"POST /run-advanced-data-refresh/ - params: {refresh_request.model_dump(exclude_none=True)}")
    
    try:
        task = advanced_data_refresh_task.delay(
            channel_ids=refresh_request.channel_ids,
            post_refresh_mode_str=refresh_request.post_refresh_mode.value,
            post_refresh_days=refresh_request.post_refresh_days,
            post_refresh_start_date_iso=refresh_request.post_refresh_start_date_str, 
            post_limit_per_channel=refresh_request.post_limit_per_channel,
            update_existing_posts_info=refresh_request.update_existing_posts_info,
            comment_refresh_mode_str=refresh_request.comment_refresh_mode.value,
            comment_limit_per_post=refresh_request.comment_limit_per_post,
            analyze_new_comments=refresh_request.analyze_new_comments
        )
        
        endpoint_logger.info(f"Advanced data refresh task '{task.id}' enqueued.")
        
        return ui_schemas.AdvancedDataRefreshResponse(
            message="Задача продвинутого обновления данных успешно поставлена в очередь.",
            task_id=task.id,
            details=refresh_request.model_dump(exclude_none=True) # exclude_none=True, чтобы не передавать None значения в 'details'
        )
    except Exception as e:
        endpoint_logger.error(f"Ошибка при постановке задачи advanced_data_refresh_task в очередь: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Не удалось запустить задачу продвинутого обновления: {str(e)}"
        )

app.include_router(api_v1_router)

@app.get("/")
async def root(): 
    return {"message": f"Welcome to {settings.PROJECT_NAME} API. Version: 0.1.0"}