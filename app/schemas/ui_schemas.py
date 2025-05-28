# app/schemas/ui_schemas.py

from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional, Any # Добавил Any для возможного использования в будущем, если понадобится

class ChannelInfo(BaseModel):
    id: int
    title: str 
    username: Optional[str] = None

    class Config:
        from_attributes = True

class PostListItem(BaseModel):
    id: int
    channel: ChannelInfo 
    post_text: Optional[str] = None
    posted_at: datetime 
    comments_count: int
    link: str 
    summary_text: Optional[str] = None
    
    # --- НОВЫЕ ОПЦИОНАЛЬНЫЕ ПОЛЯ ДЛЯ ТОНАЛЬНОСТИ ПОСТА ---
    post_sentiment_label: Optional[str] = None
    post_sentiment_score: Optional[float] = None # float для оценки
    # --- КОНЕЦ НОВЫХ ПОЛЕЙ ---


    class Config:
        from_attributes = True

class PaginatedPostsResponse(BaseModel):
    total_posts: int
    posts: List[PostListItem]

class CommentListItem(BaseModel):
    id: int
    author_display_name: str
    text: str 
    commented_at: datetime 

    class Config:
        from_attributes = True

class PaginatedCommentsResponse(BaseModel):
    total_comments: int
    comments: List[CommentListItem]