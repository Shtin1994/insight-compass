# app/main.py

import asyncio
import logging
from datetime import datetime, timedelta, timezone, date 
from typing import List 

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, asc, text, cast, Date as SQLDate, literal_column, nullslast 
from sqlalchemy.orm import selectinload 

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

app = FastAPI(title="Insight Compass API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"],    
    allow_headers=["*"],    
)

@app.get("/api/v1/dashboard/stats", response_model=ui_schemas.DashboardStatsResponse)
async def get_dashboard_stats(db: AsyncSession = Depends(get_async_db)):
    endpoint_logger.info("GET /api/v1/dashboard/stats")
    try:
        total_posts_stmt = select(func.count(models_module.Post.id))
        total_posts_res = await db.execute(total_posts_stmt)
        total_posts_all_time = total_posts_res.scalar_one_or_none() or 0
        total_comments_stmt = select(func.count(models_module.Comment.id))
        total_comments_res = await db.execute(total_comments_stmt)
        total_comments_all_time = total_comments_res.scalar_one_or_none() or 0
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        posts_last_7_stmt = select(func.count(models_module.Post.id)).where(models_module.Post.posted_at >= seven_days_ago)
        posts_last_7_res = await db.execute(posts_last_7_stmt)
        posts_last_7_days = posts_last_7_res.scalar_one_or_none() or 0
        comments_last_7_stmt = select(func.count(models_module.Comment.id)).where(models_module.Comment.commented_at >= seven_days_ago)
        comments_last_7_res = await db.execute(comments_last_7_stmt)
        comments_last_7_days = comments_last_7_res.scalar_one_or_none() or 0
        channels_count_stmt = select(func.count(models_module.Channel.id)).where(models_module.Channel.is_active == True)
        channels_count_res = await db.execute(channels_count_stmt)
        channels_monitoring_count = channels_count_res.scalar_one_or_none() or 0
        return ui_schemas.DashboardStatsResponse(
            total_posts_all_time=total_posts_all_time,
            total_comments_all_time=total_comments_all_time,
            posts_last_7_days=posts_last_7_days,
            comments_last_7_days=comments_last_7_days,
            channels_monitoring_count=channels_monitoring_count
        )
    except Exception as e:
        endpoint_logger.error(f"Error in get_dashboard_stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching dashboard stats")

@app.get("/api/v1/dashboard/activity_over_time", response_model=ui_schemas.ActivityOverTimeResponse)
async def get_activity_over_time(
    days: int = Query(7, ge=1, le=90, description="Количество дней для отображения активности (макс 90)"),
    db: AsyncSession = Depends(get_async_db)
):
    endpoint_logger.info(f"GET /api/v1/dashboard/activity_over_time?days={days}")
    try:
        start_date_dt = datetime.now(timezone.utc) - timedelta(days=days -1) 
        start_date = start_date_dt.date()
        posts_activity_stmt = (
            select(
                cast(models_module.Post.posted_at, SQLDate).label("activity_day"),
                func.count(models_module.Post.id).label("count")
            )
            .where(cast(models_module.Post.posted_at, SQLDate) >= start_date)
            .group_by(cast(models_module.Post.posted_at, SQLDate))
            .order_by(cast(models_module.Post.posted_at, SQLDate).asc())
        )
        posts_res = await db.execute(posts_activity_stmt)
        posts_by_day = {row.activity_day: row.count for row in posts_res.all()}
        comments_activity_stmt = (
            select(
                cast(models_module.Comment.commented_at, SQLDate).label("activity_day"),
                func.count(models_module.Comment.id).label("count")
            )
            .where(cast(models_module.Comment.commented_at, SQLDate) >= start_date)
            .group_by(cast(models_module.Comment.commented_at, SQLDate))
            .order_by(cast(models_module.Comment.commented_at, SQLDate).asc())
        )
        comments_res = await db.execute(comments_activity_stmt)
        comments_by_day = {row.activity_day: row.count for row in comments_res.all()}
        activity_data: List[ui_schemas.ActivityOverTimePoint] = []
        current_date_iter = start_date
        end_date = datetime.now(timezone.utc).date() 
        while current_date_iter <= end_date:
            activity_data.append(
                ui_schemas.ActivityOverTimePoint(
                    activity_date=current_date_iter,
                    post_count=posts_by_day.get(current_date_iter, 0),
                    comment_count=comments_by_day.get(current_date_iter, 0)
                )
            )
            current_date_iter += timedelta(days=1)
        return ui_schemas.ActivityOverTimeResponse(data=activity_data)
    except Exception as e:
        endpoint_logger.error(f"Error in get_activity_over_time: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching activity over time")

@app.get("/api/v1/dashboard/top_channels", response_model=ui_schemas.TopChannelsResponse)
async def get_top_channels(
    metric: str = Query("posts", pattern="^(posts|comments)$", description="Метрика для топа: 'posts' или 'comments'"),
    limit: int = Query(5, ge=1, le=20, description="Количество каналов в топе"),
    days_period: int = Query(7, ge=1, le=365, description="Период в днях для расчета топа"),
    db: AsyncSession = Depends(get_async_db)
):
    endpoint_logger.info(f"GET /api/v1/dashboard/top_channels?metric={metric}&limit={limit}&days_period={days_period}")
    try:
        start_date = datetime.now(timezone.utc) - timedelta(days=days_period)
        if metric == "posts":
            stmt = (
                select(
                    models_module.Channel.id.label("channel_id"),
                    models_module.Channel.title.label("channel_title"),
                    models_module.Channel.username.label("channel_username"),
                    func.count(models_module.Post.id).label("metric_value")
                )
                .join(models_module.Post, models_module.Channel.id == models_module.Post.channel_id)
                .where(models_module.Post.posted_at >= start_date)
                .group_by(models_module.Channel.id, models_module.Channel.title, models_module.Channel.username)
                .order_by(desc(literal_column("metric_value"))) 
                .limit(limit)
            )
        elif metric == "comments":
            stmt = (
                select(
                    models_module.Channel.id.label("channel_id"),
                    models_module.Channel.title.label("channel_title"),
                    models_module.Channel.username.label("channel_username"),
                    func.count(models_module.Comment.id).label("metric_value")
                )
                .join(models_module.Post, models_module.Channel.id == models_module.Post.channel_id)
                .join(models_module.Comment, models_module.Post.id == models_module.Comment.post_id)
                .where(models_module.Comment.commented_at >= start_date) 
                .group_by(models_module.Channel.id, models_module.Channel.title, models_module.Channel.username)
                .order_by(desc(literal_column("metric_value")))
                .limit(limit)
            )
        else: 
            raise HTTPException(status_code=400, detail="Invalid metric type specified.")
        result = await db.execute(stmt)
        top_channels_data = result.all()
        data_list = [
            ui_schemas.TopChannelItem(
                channel_id=row.channel_id,
                channel_title=row.channel_title,
                channel_username=row.channel_username,
                metric_value=row.metric_value
            ) for row in top_channels_data
        ]
        return ui_schemas.TopChannelsResponse(metric_name=metric, data=data_list)
    except Exception as e:
        endpoint_logger.error(f"Error in get_top_channels: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching top channels")

# === ДОБАВЛЕННЫЙ РАНЕЕ, НО ПРОПУЩЕННЫЙ В ПОЛНОМ КОДЕ ЭНДПОИНТ ===
@app.get("/api/v1/dashboard/sentiment_distribution", response_model=ui_schemas.SentimentDistributionResponse)
async def get_sentiment_distribution(
    days_period: int = Query(7, ge=1, le=365, description="Период в днях для анализа тональности"),
    db: AsyncSession = Depends(get_async_db)
):
    endpoint_logger.info(f"GET /api/v1/dashboard/sentiment_distribution?days_period={days_period}")
    try:
        start_date = datetime.now(timezone.utc) - timedelta(days=days_period)

        total_analyzed_stmt = (
            select(func.count(models_module.Post.id))
            .where(models_module.Post.posted_at >= start_date)
            .where(models_module.Post.post_sentiment_label != None)
        )
        total_analyzed_res = await db.execute(total_analyzed_stmt)
        total_analyzed_posts = total_analyzed_res.scalar_one_or_none() or 0

        stmt = (
            select(
                models_module.Post.post_sentiment_label.label("sentiment_label"),
                func.count(models_module.Post.id).label("count")
            )
            .where(models_module.Post.posted_at >= start_date)
            .group_by(models_module.Post.post_sentiment_label)
            .order_by(nullslast(models_module.Post.post_sentiment_label.asc())) 
        )
        
        result = await db.execute(stmt)
        sentiment_counts = result.all()

        data_list: List[ui_schemas.SentimentDistributionItem] = []
        
        # Включаем все возможные метки, чтобы в ответе всегда был полный набор
        defined_labels = ["positive", "negative", "neutral", "mixed"]
        counts_map = {row.sentiment_label: row.count for row in sentiment_counts if row.sentiment_label is not None}
        
        undefined_count = 0
        for row in sentiment_counts:
            if row.sentiment_label is None:
                undefined_count = row.count
                break
        
        for label in defined_labels:
            count = counts_map.get(label, 0)
            percentage = round((count / total_analyzed_posts) * 100, 2) if total_analyzed_posts > 0 else 0.0
            data_list.append(ui_schemas.SentimentDistributionItem(
                sentiment_label=label,
                count=count,
                percentage=percentage
            ))
            
        if undefined_count > 0:
             data_list.append(ui_schemas.SentimentDistributionItem(
                sentiment_label="undefined", # Для постов без анализа тональности
                count=undefined_count,
                percentage=0.0 # Процент для "undefined" обычно не так важен или считается иначе
            ))

        return ui_schemas.SentimentDistributionResponse(
            total_analyzed_posts=total_analyzed_posts, 
            data=data_list
        )
    except Exception as e:
        endpoint_logger.error(f"Error in get_sentiment_distribution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching sentiment distribution")
# === КОНЕЦ ДОБАВЛЕННОГО РАНЕЕ ЭНДПОИНТА ===

def get_comment_author_display_name(comment: Comment) -> str: 
    if hasattr(comment, 'author_signature') and comment.author_signature:
        return comment.author_signature
    if hasattr(comment, 'author_id') and comment.author_id:
        return f"User_{str(comment.author_id)}"
    return "Unknown Author"

@app.get("/api/v1/posts/", response_model=ui_schemas.PaginatedPostsResponse)
async def get_posts_for_ui(
    skip: int = Query(0, ge=0, description="Number of posts to skip"), 
    limit: int = Query(10, ge=1, le=100, description="Number of posts to return per page"), 
    db: AsyncSession = Depends(get_async_db)
):
    endpoint_logger.info(f"GET /api/v1/posts/ - skip={skip}, limit={limit}")
    try:
        CurrentPostModel = models_module.Post 
        total_posts_stmt = select(func.count(CurrentPostModel.id)) 
        total_posts_result = await db.execute(total_posts_stmt)
        total_posts = total_posts_result.scalar_one_or_none() or 0
        posts_stmt = (
            select(CurrentPostModel)
            .options(selectinload(CurrentPostModel.channel)) 
            .order_by(desc(CurrentPostModel.posted_at)) 
            .offset(skip)
            .limit(limit)
        )
        results = await db.execute(posts_stmt)
        posts_scalars = results.scalars().unique().all()
        posts_list = [ui_schemas.PostListItem.model_validate(post) for post in posts_scalars]
        endpoint_logger.info(f"Returning {len(posts_list)} posts, total count: {total_posts}.")
        return ui_schemas.PaginatedPostsResponse(total_posts=total_posts, posts=posts_list)
    except Exception as e:
        endpoint_logger.error(f"Error in get_posts_for_ui: {e}", exc_info=True)
        if hasattr(e, 'errors') and callable(e.errors): 
             endpoint_logger.error(f"Pydantic ValidationError details: {e.errors()}")
        raise HTTPException(status_code=500, detail="Internal server error while fetching posts")

@app.get("/api/v1/posts/{post_id}/comments/", response_model=ui_schemas.PaginatedCommentsResponse)
async def get_comments_for_post_ui(
    post_id: int,
    skip: int = Query(0, ge=0, description="Number of comments to skip"),
    limit: int = Query(10, ge=1, le=100, description="Number of comments to return per page"),
    db: AsyncSession = Depends(get_async_db)
):
    endpoint_logger.info(f"GET /api/v1/posts/{post_id}/comments/ - skip={skip}, limit={limit}")
    try:
        CurrentPostModel = models_module.Post
        CurrentCommentModel = models_module.Comment
        post_exists_stmt = select(CurrentPostModel).where(CurrentPostModel.id == post_id) 
        post_result = await db.execute(post_exists_stmt); post = post_result.scalar_one_or_none()
        if not post: 
            endpoint_logger.warning(f"Post with ID {post_id} not found for comments.")
            raise HTTPException(status_code=404, detail=f"Post with ID {post_id} not found")
        total_comments_stmt = select(func.count(CurrentCommentModel.id)).where(CurrentCommentModel.post_id == post_id) 
        total_comments_result = await db.execute(total_comments_stmt)
        total_comments = total_comments_result.scalar_one_or_none() or 0
        comments_stmt = (
            select(CurrentCommentModel) 
            .where(CurrentCommentModel.post_id == post_id) 
            .order_by(asc(CurrentCommentModel.commented_at)) 
            .offset(skip)
            .limit(limit)
        )
        comments_results = await db.execute(comments_stmt); comments_scalars = comments_results.scalars().all() 
        comments_list = []
        for comment_model_instance in comments_scalars: 
            author_name = get_comment_author_display_name(comment_model_instance) 
            comment_data = ui_schemas.CommentListItem(
                id=comment_model_instance.id, 
                author_display_name=author_name, 
                text=comment_model_instance.text_content, 
                commented_at=comment_model_instance.commented_at
            )
            comments_list.append(comment_data)
        endpoint_logger.info(f"Returning {len(comments_list)} comments for post {post_id}, total count: {total_comments}.")
        return ui_schemas.PaginatedCommentsResponse(total_comments=total_comments, comments=comments_list)
    except Exception as e:
        endpoint_logger.error(f"Error in get_comments_for_post_ui (post_id={post_id}): {e}", exc_info=True)
        if hasattr(e, 'errors') and callable(e.errors): 
             endpoint_logger.error(f"Pydantic ValidationError details: {e.errors()}")
        raise HTTPException(status_code=500, detail=f"Internal server error while fetching comments for post {post_id}")

@app.post("/run-collection-task/", summary="Запустить задачу сбора данных из Telegram")
async def run_collection_task_endpoint(background_tasks: BackgroundTasks): 
    logger.info("Endpoint /run-collection-task/ called.")
    tasks.collect_telegram_data_task.delay()
    return {"message": "Задача сбора данных запущена."}

@app.post("/run-summarization-task/", summary="Запустить задачу суммаризации топ постов")
async def run_summarization_task_endpoint(background_tasks: BackgroundTasks): 
    logger.info("Endpoint /run-summarization-task/ called.")
    tasks.summarize_top_posts_task.delay()
    return {"message": "Задача суммаризации запущена."}

@app.post("/run-daily-digest-task/", summary="Запустить задачу отправки ежедневного дайджеста")
async def run_daily_digest_task_endpoint(background_tasks: BackgroundTasks): 
    logger.info("Endpoint /run-daily-digest-task/ called.")
    tasks.send_daily_digest_task.delay()
    return {"message": "Задача дайджеста запущена."}

@app.post("/run-sentiment-analysis-task/", summary="Запустить задачу анализа тональности постов")
async def run_sentiment_analysis_task_endpoint(): 
    tasks.analyze_posts_sentiment_task.delay(limit_posts_to_analyze=2) 
    return {"message": "Задача анализа тональности запущена."}

@app.get("/")
async def root(): 
    return {"message": "Welcome to Insight Compass API"}