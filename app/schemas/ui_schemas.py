# app/schemas/ui_schemas.py

from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional # Убедитесь, что List импортирован

# Схема для отображения канала в посте
class ChannelInfo(BaseModel):
    id: int
    name: str
    username: Optional[str] = None

    class Config:
        from_attributes = True

# Схема для отображения поста в списке
class PostListItem(BaseModel):
    id: int
    channel: ChannelInfo 
    text: str
    post_date: datetime
    comments_count: int
    url: str
    summary_text: Optional[str] = None

    class Config:
        from_attributes = True

# Схема для ответа API со списком постов и пагинацией
class PaginatedPostsResponse(BaseModel):
    total_posts: int
    posts: List[PostListItem]

# Схема для отображения комментария (НОВОЕ)
class CommentListItem(BaseModel):
    id: int
    author_display_name: str # Будем формировать на бэкенде
    text: str
    comment_date: datetime

    class Config:
        from_attributes = True

# Схема для ответа API со списком комментариев и пагинацией (НОВОЕ)
class PaginatedCommentsResponse(BaseModel):
    total_comments: int
    comments: List[CommentListItem]