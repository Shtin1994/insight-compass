# app/schemas/ui_schemas.py

from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import List, Optional, Any, Dict

# --- Существующие схемы ---

class ChannelInfo(BaseModel):
    id: int # В вашей модели Channel id - BigInteger, но Pydantic int обычно справляется. Если будут проблемы, можно использовать conint.
    title: str
    username: Optional[str] = None
    class Config:
        from_attributes = True

class PostListItem(BaseModel):
    id: int
    channel: ChannelInfo
    text_content: Optional[str] = None
    caption_text: Optional[str] = None
    posted_at: datetime
    comments_count: int
    link: str
    summary_text: Optional[str] = None
    post_sentiment_label: Optional[str] = None
    post_sentiment_score: Optional[float] = None
    
    views_count: Optional[int] = None
    forwards_count: Optional[int] = None
    reactions: Optional[List[Dict[str, Any]]] = None 
    
    media_type: Optional[str] = None
    reply_to_telegram_post_id: Optional[int] = None
    author_signature: Optional[str] = None
    sender_user_id: Optional[int] = None 
    grouped_id: Optional[int] = None       
    edited_at: Optional[datetime] = None
    is_pinned: Optional[bool] = None

    class Config:
        from_attributes = True

class PaginatedPostsResponse(BaseModel):
    total_posts: int
    posts: List[PostListItem]

class CommentListItem(BaseModel):
    id: int
    author_display_name: str # Это поле формируется в эндпоинте, его нет в модели Comment напрямую
    text: Optional[str] = None # Изменил на Optional, т.к. text_content в модели теперь nullable
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
    activity_date: date
    post_count: int
    comment_count: int

class ActivityOverTimeResponse(BaseModel):
    data: List[ActivityOverTimePoint]

class TopChannelItem(BaseModel):
    channel_id: int # Аналогично ChannelInfo.id
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

class ChannelBase(BaseModel):
    pass

class ChannelCreateRequest(BaseModel):
    identifier: str = Field(..., description="Telegram channel username (e.g., 'durov') or full link (e.g., 'https://t.me/durov')")

class ChannelUpdateRequest(BaseModel):
    is_active: Optional[bool] = None

class ChannelResponse(ChannelBase):
    id: int # Аналогично ChannelInfo.id
    title: str
    username: Optional[str] = None
    description: Optional[str] = None
    is_active: bool
    last_processed_post_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class ChannelListItem(BaseModel): # Переименовал из ChannelListItem в ChannelResponse, если это основная схема для отображения канала. Если это укороченная для списков, то имя ChannelListItem ок.
    id: int # Аналогично ChannelInfo.id
    title: str
    username: Optional[str] = None
    is_active: bool
    
    class Config:
        from_attributes = True

class PaginatedChannelsResponse(BaseModel):
    total_channels: int
    channels: List[ChannelListItem] # Используем ChannelListItem

# --- НАЧАЛО: Новые схемы для AI-инсайтов из комментариев ---
class InsightItem(BaseModel):
    text: str  # Текст темы, проблемы, вопроса или предложения
    count: int # Количество упоминаний

class CommentInsightsResponse(BaseModel):
    period_days: int
    top_topics: List[InsightItem]
    top_problems: List[InsightItem]
    top_questions: List[InsightItem]
    top_suggestions: List[InsightItem]
# --- КОНЕЦ: Новые схемы для AI-инсайтов из комментариев ---