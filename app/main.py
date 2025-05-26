# app/main.py

import asyncio
import logging
from datetime import datetime, timedelta

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, asc

from . import tasks, models
from .db.session import get_async_db, async_engine 
# ИЗМЕНЕНО: импортируем celery_app вместо celery_worker
from .celery_app import celery_app 
from .core.config import settings
from .models import Base 
from .schemas import ui_schemas

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Insight Compass API", version="0.1.0")


@app.get("/api/v1/posts/", response_model=ui_schemas.PaginatedPostsResponse)
async def get_posts_for_ui(
    skip: int = Query(0, ge=0, description="Number of posts to skip"), 
    limit: int = Query(10, ge=1, le=100, description="Number of posts to return per page"), 
    db: AsyncSession = Depends(get_async_db)
):
    """
    Получает список постов для отображения в UI с пагинацией.
    Посты отсортированы по дате публикации (сначала новые).
    """
    try:
        total_posts_stmt = select(func.count(models.Post.id))
        total_posts_result = await db.execute(total_posts_stmt)
        total_posts = total_posts_result.scalar_one_or_none() or 0

        posts_stmt = (
            select(models.Post)
            .join(models.Post.channel) 
            .order_by(desc(models.Post.post_date))
            .offset(skip)
            .limit(limit)
        )
        
        results = await db.execute(posts_stmt)
        posts_scalars = results.scalars().all()
        
        posts_list = [ui_schemas.PostListItem.model_validate(post) for post in posts_scalars]

        return ui_schemas.PaginatedPostsResponse(total_posts=total_posts, posts=posts_list)
    except Exception as e:
        logger.error(f"Error fetching posts for UI: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while fetching posts")


def get_comment_author_display_name(comment: models.Comment) -> str:
    """
    Формирует отображаемое имя автора комментария.
    """
    if comment.username:
        return comment.username
    if comment.first_name and comment.last_name:
        return f"{comment.first_name} {comment.last_name}"
    if comment.first_name:
        return comment.first_name
    if comment.last_name: 
        return comment.last_name
    if comment.user_id:
        return f"User_{str(comment.user_id)}"
    return "Unknown Author"

@app.get("/api/v1/posts/{post_id}/comments/", response_model=ui_schemas.PaginatedCommentsResponse)
async def get_comments_for_post_ui(
    post_id: int,
    skip: int = Query(0, ge=0, description="Number of comments to skip"),
    limit: int = Query(10, ge=1, le=100, description="Number of comments to return per page"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Получает список комментариев для конкретного поста с пагинацией.
    Комментарии отсортированы по дате публикации (сначала старые).
    """
    try:
        post_exists_stmt = select(models.Post).where(models.Post.id == post_id)
        post_result = await db.execute(post_exists_stmt)
        post = post_result.scalar_one_or_none()
        if not post:
            raise HTTPException(status_code=404, detail=f"Post with ID {post_id} not found")

        total_comments_stmt = select(func.count(models.Comment.id)).where(models.Comment.post_id == post_id)
        total_comments_result = await db.execute(total_comments_stmt)
        total_comments = total_comments_result.scalar_one_or_none() or 0

        comments_stmt = (
            select(models.Comment)
            .where(models.Comment.post_id == post_id)
            .order_by(asc(models.Comment.comment_date)) 
            .offset(skip)
            .limit(limit)
        )
        comments_results = await db.execute(comments_stmt)
        comments_scalars = comments_results.scalars().all()

        comments_list = []
        for comment_model in comments_scalars:
            author_name = get_comment_author_display_name(comment_model)
            comment_data = ui_schemas.CommentListItem(
                id=comment_model.id,
                author_display_name=author_name,
                text=comment_model.text,
                comment_date=comment_model.comment_date
            )
            comments_list.append(comment_data)

        return ui_schemas.PaginatedCommentsResponse(total_comments=total_comments, comments=comments_list)
    except HTTPException: 
        raise
    except Exception as e:
        logger.error(f"Error fetching comments for post {post_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error while fetching comments for post {post_id}")


@app.post("/run-collection-task/", summary="Запустить задачу сбора данных из Telegram")
async def run_collection_task_endpoint(background_tasks: BackgroundTasks):
    logger.info("Endpoint /run-collection-task/ called. Adding task to background.")
    tasks.collect_telegram_data_task.delay()
    return {"message": "Задача сбора данных запущена в фоновом режиме."}

@app.post("/run-summarization-task/", summary="Запустить задачу суммаризации топ постов")
async def run_summarization_task_endpoint(background_tasks: BackgroundTasks):
    logger.info("Endpoint /run-summarization-task/ called. Adding task to background.")
    tasks.summarize_top_posts_task.delay()
    return {"message": "Задача суммаризации топ постов запущена в фоновом режиме."}

@app.post("/run-daily-digest-task/", summary="Запустить задачу отправки ежедневного дайджеста")
async def run_daily_digest_task_endpoint(background_tasks: BackgroundTasks):
    logger.info("Endpoint /run-daily-digest-task/ called. Adding task to background.")
    tasks.send_daily_digest_task.delay()
    return {"message": "Задача отправки ежедневного дайджеста запущена в фоновом режиме."}

@app.get("/")
async def root():
    return {"message": "Welcome to Insight Compass API"}