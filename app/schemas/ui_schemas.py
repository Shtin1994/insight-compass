# app/schemas/ui_schemas.py

from pydantic import BaseModel
from datetime import datetime, date 
from typing import List, Optional, Any 

class ChannelInfo(BaseModel):
    id: int
    title: str 
    username: Optional[str] = None
    class Config: from_attributes = True

class PostListItem(BaseModel):
    id: int
    channel: ChannelInfo 
    post_text: Optional[str] = None
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

# --- НОВЫЕ СХЕМЫ ДЛЯ РАСПРЕДЕЛЕНИЯ ТОНАЛЬНОСТИ ---
class SentimentDistributionItem(BaseModel):
    sentiment_label: str  # "positive", "negative", "neutral", "mixed", или "undefined" для тех, что еще не проанализированы
    count: int
    percentage: float # Процент от общего числа постов с проанализированной тональностью (или от всех за период)

class SentimentDistributionResponse(BaseModel):
    total_analyzed_posts: int # Общее количество постов, для которых есть метка тональности за период
    data: List[SentimentDistributionItem]
# --- КОНЕЦ НОВЫХ СХЕМ ---