# app/schemas/ui_schemas.py

from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import List, Optional, Any

# --- Существующие схемы ---

class ChannelInfo(BaseModel):
    id: int
    title: str
    username: Optional[str] = None
    class Config: from_attributes = True

class PostListItem(BaseModel):
    id: int
    channel: ChannelInfo
    text_content: Optional[str] = None # <--- ИЗМЕНЕНО ИМЯ ПОЛЯ
    posted_at: datetime
    comments_count: int
    link: str
    summary_text: Optional[str] = None
    post_sentiment_label: Optional[str] = None
    post_sentiment_score: Optional[float] = None
    class Config: from_attributes = True

class PaginatedPostsResponse(BaseModel):
    total_posts: int
    posts: List[PostListItem]

class CommentListItem(BaseModel):
    id: int
    author_display_name: str
    text: str
    commented_at: datetime
    class Config: from_attributes = True

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
    activity_date: date
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
    sentiment_label: str
    count: int
    percentage: float

class SentimentDistributionResponse(BaseModel):
    total_analyzed_posts: int
    data: List[SentimentDistributionItem]

# --- НОВЫЕ СХЕМЫ ДЛЯ УПРАВЛЕНИЯ КАНАЛАМИ ---

class ChannelBase(BaseModel):
    # Базовая схема, от которой могут наследоваться другие.
    # Здесь можно определить общие поля, если они есть.
    # В данном случае, для создания используется только identifier,
    # а для ответа поля берутся из модели.
    pass

class ChannelCreateRequest(BaseModel):
    # Что пользователь отправляет для добавления нового канала
    # Пользователь может указать либо username, либо полную ссылку на канал
    identifier: str = Field(..., description="Telegram channel username (e.g., 'durov') or full link (e.g., 'https://t.me/durov')")

class ChannelUpdateRequest(BaseModel):
    # Что пользователь может обновить у канала
    # На данный момент, основной сценарий - активация/деактивация
    is_active: Optional[bool] = None
    # В будущем можно добавить, например, кастомное имя для канала в системе
    # custom_title: Optional[str] = None

class ChannelResponse(ChannelBase):
    # Что мы возвращаем пользователю при запросе информации о канале
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
    # Сокращенная информация для списка каналов
    id: int # Telegram ID
    title: str
    username: Optional[str] = None
    is_active: bool
    # Можно добавить другие поля по необходимости, например, когда последний раз были собраны данные
    # last_fetched_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class PaginatedChannelsResponse(BaseModel):
    total_channels: int
    channels: List[ChannelListItem]