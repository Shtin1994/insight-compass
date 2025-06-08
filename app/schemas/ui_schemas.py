# app/schemas/ui_schemas.py

from pydantic import BaseModel, Field, validator, field_validator # Добавил field_validator для >Pydantic V2
from datetime import datetime, date
from typing import List, Optional, Any, Dict
from enum import Enum
from app.core.config import settings # Импортируем settings для доступа к COMMENT_FETCH_LIMIT

# --- Существующие схемы ---

class ChannelInfo(BaseModel):
    id: int 
    title: str
    username: Optional[str] = None

    class Config:
        from_attributes = True # Для SQLAlchemy моделей

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
    author_display_name: str 
    text: Optional[str] = None 
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
    activity_date: date # Используем date, а не datetime для агрегации по дням
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

class ChannelBase(BaseModel): # Базовая схема для канала, если понадобится
    pass

class ChannelCreateRequest(BaseModel):
    identifier: str = Field(..., description="Telegram channel username (e.g., 'durov') or full link (e.g., 'https://t.me/durov') or ID")

class ChannelUpdateRequest(BaseModel):
    is_active: Optional[bool] = None
    # Можно добавить другие поля для обновления, если нужно

class ChannelResponse(ChannelBase):
    id: int 
    title: str
    username: Optional[str] = None
    description: Optional[str] = None
    is_active: bool
    last_processed_post_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class ChannelListItem(BaseModel): # Используется в PaginatedChannelsResponse
    id: int 
    title: str
    username: Optional[str] = None
    is_active: bool
    # last_processed_post_id: Optional[int] = None # Можно добавить, если нужно на фронте

    class Config:
        from_attributes = True

class PaginatedChannelsResponse(BaseModel):
    total_channels: int
    channels: List[ChannelListItem]

class InsightItem(BaseModel):
    text: str
    count: int

class CommentInsightsResponse(BaseModel):
    period_days: int
    top_topics: List[InsightItem]
    top_problems: List[InsightItem]
    top_questions: List[InsightItem]
    top_suggestions: List[InsightItem]

class InsightItemType(str, Enum):
    TOPIC = "topic"
    PROBLEM = "problem"
    QUESTION = "question"
    SUGGESTION = "suggestion"

class TrendGranularity(str, Enum):
    DAY = "day"
    WEEK = "week"
    # MONTH = "month" # Можно добавить

class InsightTrendDataPoint(BaseModel):
    date: str # YYYY-MM-DD для дня, YYYY-WW для недели
    count: int

class InsightItemTrendResponse(BaseModel):
    item_type: InsightItemType
    item_text: str
    period_days: int
    granularity: TrendGranularity
    trend_data: List[InsightTrendDataPoint]

class NLQueryRequest(BaseModel):
    query_text: str = Field(..., min_length=3, description="Текст вопроса пользователя на естественном языке")
    days_period: Optional[int] = Field(None, ge=1, le=365, description="Период в днях для контекста (если релевантно)")

class NLQueryResponse(BaseModel):
    original_query: str
    ai_answer: str = Field(..., description="Ответ, сгенерированный AI на основе вопроса и данных")

# --- НАЧАЛО: Схемы для "Продвинутого обновления данных" ---

class PostRefreshMode(str, Enum):
    NEW_ONLY = "new_only"
    LAST_N_DAYS = "last_n_days"
    SINCE_DATE = "since_date"
    UPDATE_STATS_ONLY = "update_stats_only"

class CommentRefreshMode(str, Enum):
    NEW_POSTS_ONLY = "new_posts_only"
    ADD_NEW_TO_EXISTING = "add_new_to_existing"
    DO_NOT_REFRESH = "do_not_refresh"

class AdvancedDataRefreshRequest(BaseModel):
    channel_ids: Optional[List[int]] = Field(None, description="Список ID каналов для обновления. Если None или пустой список - все активные.")
    
    post_refresh_mode: PostRefreshMode = Field(default=PostRefreshMode.NEW_ONLY, description="Режим обновления постов.")
    post_refresh_days: Optional[int] = Field(None, ge=1, le=365, description="Количество дней для режима LAST_N_DAYS.")
    post_refresh_start_date_str: Optional[str] = Field(None, description="Дата начала для режима SINCE_DATE (YYYY-MM-DD).")
    post_limit_per_channel: int = Field(default=100, ge=10, le=5000, description="Лимит постов на канал для режимов, где это применимо.")
    
    update_existing_posts_info: bool = Field(default=False, description="Обновлять ли информацию для уже существующих в БД постов.")
    
    comment_refresh_mode: CommentRefreshMode = Field(default=CommentRefreshMode.ADD_NEW_TO_EXISTING, description="Режим обновления комментариев.")
    comment_limit_per_post: int = Field(default=settings.COMMENT_FETCH_LIMIT, ge=10, le=1000, description="Лимит комментариев на пост для сбора.")

    analyze_new_comments: bool = Field(default=True, description="Запускать ли AI-анализ для новых комментариев после сбора.")

    @field_validator('post_refresh_days', mode='before') # Используем field_validator для Pydantic v2
    @classmethod
    def check_post_refresh_days(cls, v, values):
        # Pydantic v2: values - это уже объект данных, а не словарь
        data = values.data if hasattr(values, 'data') else values # Обработка для совместимости
        if data.get('post_refresh_mode') == PostRefreshMode.LAST_N_DAYS and v is None:
            raise ValueError('post_refresh_days is required when post_refresh_mode is "last_n_days"')
        return v

    @field_validator('post_refresh_start_date_str', mode='before')
    @classmethod
    def check_post_refresh_start_date_str(cls, v, values):
        data = values.data if hasattr(values, 'data') else values
        if data.get('post_refresh_mode') == PostRefreshMode.SINCE_DATE and v is None:
            raise ValueError('post_refresh_start_date_str is required when post_refresh_mode is "since_date"')
        if v is not None:
            try:
                datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                raise ValueError('post_refresh_start_date_str must be in YYYY-MM-DD format')
        return v

class AdvancedDataRefreshResponse(BaseModel):
    message: str
    task_id: Optional[str] = None 
    details: Optional[Dict[str, Any]] = None

# --- КОНЕЦ: Схемы для "Продвинутого обновления данных" ---

# --- НАЧАЛО: Схемы для пакетного AI-анализа (Кнопка 3) ---

class BatchedAIAnalysisRequest(BaseModel):
    channel_ids: Optional[List[int]] = Field(None, description="Список ID каналов для анализа. Если None или пустой список - все активные.")

class TaskInfo(BaseModel):
    task_type: str = Field(..., description="Тип запущенной задачи (например, 'sentiment_analysis', 'summarization', 'comment_feature_enqueue')")
    task_id: str = Field(..., description="ID запущенной Celery задачи")
    channel_id_processed: Optional[int] = Field(None, description="ID канала, для которого запущена задача (актуально для comment_feature_enqueue)")

class BatchedAIAnalysisResponse(BaseModel):
    message: str = Field(..., description="Сообщение о результате постановки задач в очередь")
    launched_tasks: List[TaskInfo] = Field(default_factory=list, description="Информация о запущенных Celery задачах")

# --- КОНЕЦ: Схемы для пакетного AI-анализа ---