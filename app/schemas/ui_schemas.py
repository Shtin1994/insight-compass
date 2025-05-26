# app/schemas/ui_schemas.py

from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class ChannelInfo(BaseModel):
    id: int
    title: str  # Соответствует модели Channel.title
    username: Optional[str] = None

    class Config:
        from_attributes = True

class PostListItem(BaseModel):
    id: int
    channel: ChannelInfo 
    post_text: Optional[str] = None # Соответствует модели Post.post_text
    posted_at: datetime            # Соответствует модели Post.posted_at
    comments_count: int
    link: str                      # Соответствует модели Post.link
    summary_text: Optional[str] = None

    class Config:
        from_attributes = True

class PaginatedPostsResponse(BaseModel):
    total_posts: int
    posts: List[PostListItem]

class CommentListItem(BaseModel):
    id: int
    author_display_name: str # Формируется в main.py
    text: str                # В это поле будет передаваться значение из Comment.content
    commented_at: datetime   # Соответствует модели Comment.commented_at

    class Config:
        from_attributes = True

class PaginatedCommentsResponse(BaseModel):
    total_comments: int
    comments: List[CommentListItem]