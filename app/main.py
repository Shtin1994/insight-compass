# app/main.py

import asyncio
import logging
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException, Query, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, asc, text, cast, Date as SQLDate, literal_column, nullslast, update, delete, or_
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import IntegrityError

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import Channel as TelethonChannelType, Chat as TelethonChatType, User as TelethonUserType

from . import tasks
from .models import Post, Comment, Channel
from . import models as models_module
from .db.session import get_async_db, async_engine
from .celery_app import celery_instance
from .core.config import settings
from .schemas import ui_schemas

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
endpoint_logger = logging.getLogger("api_endpoints")
endpoint_logger.setLevel(logging.INFO)

if not endpoint_logger.handlers and not logging.getLogger().handlers:
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(_formatter)
    endpoint_logger.addHandler(_handler)
elif not endpoint_logger.handlers and endpoint_logger.parent and endpoint_logger.parent.handlers:
    pass

telegram_client_instance: Optional[TelegramClient] = None

async def get_telegram_client() -> TelegramClient:
    global telegram_client_instance
    if telegram_client_instance is None or not telegram_client_instance.is_connected():
        raise HTTPException(status_code=503, detail="Telegram client is not available or not connected.")
    return telegram_client_instance

app = FastAPI(title=settings.PROJECT_NAME, version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    global telegram_client_instance
    if not all([settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH, settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN]):
        logger.error("Telegram API credentials are not fully configured. Telegram client will not be initialized.")
        return
    session_file_path = "/app/api_telegram_session"
    logger.info(f"API attempting to use Telethon session file: {session_file_path}.session")
    telegram_client_instance = TelegramClient(session_file_path, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
    try:
        logger.info("API: Connecting to Telegram via Telethon...")
        await telegram_client_instance.connect()
        if not await telegram_client_instance.is_user_authorized():
            logger.error(f"API: Telegram user is NOT authorized for session {session_file_path}.session. Please ensure this session file is created and authorized.")
        else:
            me = await telegram_client_instance.get_me()
            logger.info(f"API: Successfully connected to Telegram as {me.first_name} (@{me.username or ''})")
    except Exception as e:
        logger.error(f"API: Failed to connect to Telegram or authorize: {e}", exc_info=True)
        if telegram_client_instance and telegram_client_instance.is_connected(): await telegram_client_instance.disconnect()
        telegram_client_instance = None

@app.on_event("shutdown")
async def shutdown_event():
    global telegram_client_instance
    if telegram_client_instance and telegram_client_instance.is_connected():
        logger.info("Disconnecting Telegram client...")
        await telegram_client_instance.disconnect()

api_v1_router = APIRouter(prefix=settings.API_V1_STR)

@api_v1_router.post("/channels/", response_model=ui_schemas.ChannelResponse, status_code=201)
async def add_new_channel(channel_data: ui_schemas.ChannelCreateRequest, db: AsyncSession = Depends(get_async_db), tg_client: TelegramClient = Depends(get_telegram_client)):
    endpoint_logger.info(f"POST /channels/ - identifier: {channel_data.identifier}")
    channel_identifier = channel_data.identifier.strip()
    try: entity = await tg_client.get_entity(channel_identifier)
    except (ValueError, ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError) as e: raise HTTPException(status_code=404, detail=f"Telegram entity '{channel_identifier}' not found or inaccessible: {e}")
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error communicating with Telegram: {e}")
    if not isinstance(entity, TelethonChannelType):
        error_msg = f"Identifier '{channel_identifier}' resolves to a '{type(entity).__name__}', not a channel or supergroup."
        if isinstance(entity, TelethonChatType): error_msg = f"Identifier '{channel_identifier}' resolves to a basic group chat, not supported."
        elif isinstance(entity, TelethonUserType): error_msg = f"Identifier '{channel_identifier}' resolves to a user, not a channel."
        raise HTTPException(status_code=400, detail=error_msg)
    is_broadcast = getattr(entity, 'broadcast', False); is_super_group = getattr(entity, 'megagroup', False)
    if not (is_broadcast or is_super_group): raise HTTPException(status_code=400, detail=f"Identifier '{channel_identifier}' not a broadcast channel or supergroup.")
    existing_channel_stmt = select(models_module.Channel).where(models_module.Channel.id == entity.id)
    db_channel = (await db.execute(existing_channel_stmt)).scalar_one_or_none()
    if db_channel:
        if not db_channel.is_active:
            db_channel.is_active = True; db_channel.title = entity.title; db_channel.username = getattr(entity, 'username', None); db_channel.description = getattr(entity, 'about', None)
            db.add(db_channel); await db.commit(); await db.refresh(db_channel)
            return db_channel
        else: raise HTTPException(status_code=409, detail=f"Channel '{entity.title}' (ID: {entity.id}) already tracked.")
    new_channel = models_module.Channel(id=entity.id, username=getattr(entity, 'username', None), title=entity.title, description=getattr(entity, 'about', None), is_active=True)
    db.add(new_channel)
    try: await db.commit(); await db.refresh(new_channel)
    except IntegrityError: await db.rollback(); raise HTTPException(status_code=409, detail="Channel added by another process or conflict.")
    return new_channel

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
    db.add(channel); await db.commit(); await db.refresh(channel)
    return channel

@api_v1_router.delete("/channels/{channel_id}/", status_code=204)
async def delete_tracked_channel(channel_id: int, db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"DELETE /channels/{channel_id}/")
    channel = await db.get(models_module.Channel, channel_id)
    if not channel: raise HTTPException(status_code=404, detail="Channel not found")
    if channel.is_active: channel.is_active = False; db.add(channel); await db.commit()
    return None

# --- ВОССТАНОВЛЕННЫЕ ЭНДПОИНТЫ ДЛЯ ДАШБОРДА ---
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
        posts_by_day = {row.activity_day: row.count for row in (await db.execute(posts_activity_stmt)).all()}
        comments_activity_stmt = (select(cast(models_module.Comment.commented_at, SQLDate).label("activity_day"), func.count(models_module.Comment.id).label("count")).join(models_module.Post, models_module.Comment.post_id == models_module.Post.id).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(cast(models_module.Comment.commented_at, SQLDate) >= start_date).group_by(cast(models_module.Comment.commented_at, SQLDate)).order_by(cast(models_module.Comment.commented_at, SQLDate).asc()))
        comments_by_day = {row.activity_day: row.count for row in (await db.execute(comments_activity_stmt)).all()}
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
        top_channels_data = (await db.execute(stmt)).all()
        data_list = [ui_schemas.TopChannelItem(channel_id=row.channel_id, channel_title=row.channel_title, channel_username=row.channel_username, metric_value=row.metric_value) for row in top_channels_data]
        return ui_schemas.TopChannelsResponse(metric_name=metric, data=data_list)
    except Exception as e: endpoint_logger.error(f"Error in get_top_channels: {e}", exc_info=True); raise HTTPException(status_code=500, detail="Internal server error while fetching top channels")

@api_v1_router.get("/dashboard/sentiment_distribution", response_model=ui_schemas.SentimentDistributionResponse)
async def get_sentiment_distribution(days_period: int = Query(7, ge=1, le=365), db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /api/v1/dashboard/sentiment_distribution?days_period={days_period}")
    try:
        start_date = datetime.now(timezone.utc) - timedelta(days=days_period)
        active_channels_subquery = select(models_module.Channel.id).where(models_module.Channel.is_active == True).subquery()
        total_analyzed_stmt = (select(func.count(models_module.Post.id)).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(models_module.Post.posted_at >= start_date).where(models_module.Post.post_sentiment_label != None))
        total_analyzed_posts = (await db.execute(total_analyzed_stmt)).scalar_one_or_none() or 0
        stmt = (select(models_module.Post.post_sentiment_label.label("sentiment_label"), func.count(models_module.Post.id).label("count")).join(active_channels_subquery, models_module.Post.channel_id == active_channels_subquery.c.id).where(models_module.Post.posted_at >= start_date).group_by(models_module.Post.post_sentiment_label).order_by(nullslast(models_module.Post.post_sentiment_label.asc())))
        sentiment_counts = (await db.execute(stmt)).all(); data_list: List[ui_schemas.SentimentDistributionItem] = []
        defined_labels = ["positive", "negative", "neutral", "mixed"]; counts_map = {row.sentiment_label: row.count for row in sentiment_counts if row.sentiment_label is not None}; undefined_count = 0
        for row in sentiment_counts:
            if row.sentiment_label is None: undefined_count = row.count; break
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
# --- КОНЕЦ ВОССТАНОВЛЕННЫХ ЭНДПОИНТОВ ДЛЯ ДАШБОРДА ---

def get_comment_author_display_name(comment: models_module.Comment) -> str:
    if hasattr(comment, 'user_fullname') and comment.user_fullname and comment.user_fullname.strip(): return comment.user_fullname.strip()
    if hasattr(comment, 'user_username') and comment.user_username: return f"@{comment.user_username}"
    if hasattr(comment, 'telegram_user_id') and comment.telegram_user_id: return f"User_{str(comment.telegram_user_id)}"
    return "Unknown Author"

@api_v1_router.get("/posts/", response_model=ui_schemas.PaginatedPostsResponse)
async def get_posts_for_ui(skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=100), search_query: Optional[str] = Query(None, description="Search query to filter posts"), db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info(f"GET /api/v1/posts/ - skip={skip}, limit={limit}, search_query='{search_query}'")
    try:
        CurrentPostModel = models_module.Post; CurrentChannelModel = models_module.Channel
        base_query = select(CurrentPostModel).join(CurrentChannelModel, CurrentPostModel.channel_id == CurrentChannelModel.id).where(CurrentChannelModel.is_active == True)
        count_query = select(func.count(CurrentPostModel.id)).join(CurrentChannelModel, CurrentPostModel.channel_id == CurrentChannelModel.id).where(CurrentChannelModel.is_active == True)

        if search_query:
            search_pattern = f"%{search_query}%"
            search_condition = or_(CurrentPostModel.text_content.ilike(search_pattern), CurrentPostModel.summary_text.ilike(search_pattern))
            base_query = base_query.where(search_condition)
            count_query = count_query.where(search_condition)
        
        total_posts_result = await db.execute(count_query)
        total_posts = total_posts_result.scalar_one_or_none() or 0
        
        posts_stmt = base_query.options(selectinload(CurrentPostModel.channel)).order_by(desc(CurrentPostModel.posted_at)).offset(skip).limit(limit)
        results = await db.execute(posts_stmt)
        posts_scalars = results.scalars().unique().all()
        posts_list = [ui_schemas.PostListItem.model_validate(post) for post in posts_scalars]
        endpoint_logger.info(f"Returning {len(posts_list)} posts. Total matching: {total_posts}.")
        return ui_schemas.PaginatedPostsResponse(total_posts=total_posts, posts=posts_list)
    except Exception as e:
        endpoint_logger.error(f"Error in get_posts_for_ui: {e}", exc_info=True)
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

@api_v1_router.post("/run-collection-task/", summary="Запустить задачу сбора данных из Telegram")
async def run_collection_task_endpoint(background_tasks: BackgroundTasks):
    logger.info("Endpoint /run-collection-task/ called.")
    tasks.collect_telegram_data_task.delay()
    return {"message": "Задача сбора данных запущена."}

@api_v1_router.post("/run-summarization-task/", summary="Запустить задачу суммаризации топ постов")
async def run_summarization_task_endpoint(background_tasks: BackgroundTasks):
    logger.info("Endpoint /run-summarization-task/ called.")
    tasks.summarize_top_posts_task.delay()
    return {"message": "Задача суммаризации запущена."}

@api_v1_router.post("/run-daily-digest-task/", summary="Запустить задачу отправки ежедневного дайджеста")
async def run_daily_digest_task_endpoint(background_tasks: BackgroundTasks):
    logger.info("Endpoint /run-daily-digest-task/ called.")
    tasks.send_daily_digest_task.delay()
    return {"message": "Задача дайджеста запущена."}

@api_v1_router.post("/run-sentiment-analysis-task/", summary="Запустить задачу анализа тональности постов")
async def run_sentiment_analysis_task_endpoint():
    tasks.analyze_posts_sentiment_task.delay(limit_posts_to_analyze=settings.POST_FETCH_LIMIT or 5)
    return {"message": "Задача анализа тональности запущена."}

app.include_router(api_v1_router)

@app.get("/")
async def root():
    return {"message": f"Welcome to {settings.PROJECT_NAME} API"}