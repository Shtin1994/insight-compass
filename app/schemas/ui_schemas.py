# app/schemas/ui_schemas.py

from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import List, Optional, Any, Dict # Добавил Dict для типизации reactions

# --- Существующие схемы ---

class ChannelInfo(BaseModel):
    id: int
    title: str
    username: Optional[str] = None
    class Config:
        from_attributes = True # orm_mode в Pydantic v1

class PostListItem(BaseModel): # <--- ЗДЕСЬ ОСНОВНЫЕ ИЗМЕНЕНИЯ
    id: int
    channel: ChannelInfo
    text_content: Optional[str] = None
    caption_text: Optional[str] = None       # Текст подписи к медиа
    posted_at: datetime
    comments_count: int
    link: str
    summary_text: Optional[str] = None
    post_sentiment_label: Optional[str] = None
    post_sentiment_score: Optional[float] = None
    
    # Поля, добавленные для отладки сортировки и для большей информативности
    views_count: Optional[int] = None
    forwards_count: Optional[int] = None
    reactions: Optional[List[Dict[str, Any]]] = None # Список словарей для реакций
    
    # Другие новые поля из модели Post, которые могут быть полезны в API
    media_type: Optional[str] = None
    # media_content_info: Optional[Dict[str, Any]] = None # Если нужна детальная инфо о медиа
    reply_to_telegram_post_id: Optional[int] = None
    author_signature: Optional[str] = None
    sender_user_id: Optional[int] = None # Используем int, т.к. в модели BigInteger, но ID обычно int
    grouped_id: Optional[int] = None       # Аналогично, BigInteger в модели, но ID часто int
    edited_at: Optional[datetime] = None
    is_pinned: Optional[bool] = None # Сделаем Optional, т.к. default=False в модели

    class Config:
        from_attributes = True

class PaginatedPostsResponse(BaseModel):
    total_posts: int
    posts: List[PostListItem]

class CommentListItem(BaseModel):
    id: int
    author_display_name: str
    text: str # Основной текст комментария
    # Можно добавить и сюда caption_text, media_type, reactions для комментариев, если нужно будет выводить
    commented_at: datetime
    class Config:
        from_attributes = True

class PaginatedCommentsResponse(BaseModel):
    total_comments: int
    comments: List[CommentListItem]

class DashboardStatsResponse(BaseModel):
    total_posts_all_time: int
    total_comments_all_time: int
    posts_last_7_days: int
    comments_last_7_days: int
    channels_monitoring_count: int

class ActivityOverTimePoint(BaseModel):
    activity_date: date # Используем date, т.к. агрегация по дням
    post_count: int
    comment_count: int

class ActivityOverTimeResponse(BaseModel):
    data: List[ActivityOverTimePoint]

class TopChannelItem(BaseModel):
    channel_id: int
    channel_title: str
    channel_username: Optional[str] = None
    metric_value: int

class TopChannelsResponse(BaseModel):
    metric_name: str
    data: List[TopChannelItem]

class SentimentDistributionItem(BaseModel):
    sentiment_label: str # e.g., "positive", "negative", "neutral", "mixed", "undefined"
    count: int
    percentage: float

class SentimentDistributionResponse(BaseModel):
    total_analyzed_posts: int # Количество постов, для которых есть анализ тональности
    data: List[SentimentDistributionItem]

# --- Схемы для управления каналами ---

class ChannelBase(BaseModel):
    pass # Можно оставить пустым или добавить общие поля, если появятся

class ChannelCreateRequest(BaseModel):
    identifier: str = Field(..., description="Telegram channel username (e.g., 'durov') or full link (e.g., 'https://t.me/durov')")

class ChannelUpdateRequest(BaseModel):
    is_active: Optional[bool] = None
    # custom_title: Optional[str] = None # Пример для будущего расширения

class ChannelResponse(ChannelBase):
    id: int # Telegram ID канала, который является PK
    title: str
    username: Optional[str] = None
    description: Optional[str] = None
    is_active: bool
    last_processed_post_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class ChannelListItem(BaseModel):
    id: int # Telegram ID
    title: str
    username: Optional[str] = None
    is_active: bool
    # last_fetched_at: Optional[datetime] = None # Пример для будущего расширения

    class Config:
        from_attributes = True

class PaginatedChannelsResponse(BaseModel):
    total_channels: int
    channels: List[ChannelListItem]