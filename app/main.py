# app/main.py

import asyncio
import logging
from datetime import datetime, timedelta

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, asc
from sqlalchemy.orm import selectinload 

from . import tasks
from .models import Post, Comment, Channel 
from . import models as models_module 

from .db.session import get_async_db, async_engine 
from .celery_app import celery_instance 
from .core.config import settings
from .schemas import ui_schemas

# Основной логгер приложения
logging.basicConfig(level=logging.INFO) # Общий уровень INFO
logger = logging.getLogger(__name__)

# Логгер для эндпоинтов
endpoint_logger = logging.getLogger("api_endpoints") 
endpoint_logger.setLevel(logging.INFO) # Устанавливаем уровень INFO для endpoint_logger

# Убедимся, что у логгера есть обработчик, чтобы он выводил сообщения
if not endpoint_logger.handlers and not logging.getLogger().handlers:
    # Если ни у endpoint_logger, ни у корневого логгера нет обработчиков, добавим свой.
    # Это актуально, если logging.basicConfig не сработал должным образом или был переопределен.
    # Обычно в Docker логи uvicorn уже настроены, и это может быть излишним,
    # но для надежности оставим.
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(_formatter)
    endpoint_logger.addHandler(_handler)
elif not endpoint_logger.handlers and endpoint_logger.parent and endpoint_logger.parent.handlers:
    # Если у родительского логгера есть обработчики, наш их унаследует, если propagate=True (по умолчанию)
    pass


app = FastAPI(title="Insight Compass API", version="0.1.0")

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
        if hasattr(e, 'errors') and callable(e.errors): # Для ValidationError Pydantic
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
        post_result = await db.execute(post_exists_stmt)
        post = post_result.scalar_one_or_none()
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
        comments_results = await db.execute(comments_stmt)
        comments_scalars = comments_results.scalars().all() 

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

# ... (остальные эндпоинты POST и GET / без изменений) ...
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