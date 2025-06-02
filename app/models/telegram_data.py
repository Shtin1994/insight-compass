# app/models/telegram_data.py
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, BigInteger, Float
from sqlalchemy.dialects.postgresql import JSONB # Для хранения JSON данных
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.base_class import Base
# from typing import Optional # Для type hinting в Python, не для SQLAlchemy напрямую - можно убрать, если не используется для типизации внутри файла
from datetime import datetime as Pydatetime # Чтобы не путать с SQLAlchemy DateTime

class Channel(Base):
    __tablename__ = "channels"

    id = Column(BigInteger, primary_key=True, index=True, comment="Telegram ID канала, используется как PK")
    username = Column(String(255), index=True, nullable=True, comment="Username канала (e.g., @durov)")
    title = Column(String(255), nullable=False, comment="Название канала")
    description = Column(Text, nullable=True, comment="Описание канала")
    
    last_processed_post_id = Column(Integer, nullable=True, comment="ID последнего обработанного поста Telegram из этого канала")
    is_active = Column(Boolean, default=True, nullable=False, comment="Флаг, активен ли мониторинг")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    posts = relationship("Post", back_populates="channel", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Channel(id={self.id}, username='{self.username}', title='{self.title}')>"

class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True, comment="Внутренний автоинкрементный ID поста")
    telegram_post_id = Column(Integer, index=True, nullable=False, comment="ID поста в Telegram")
    channel_id = Column(BigInteger, ForeignKey("channels.id", ondelete="CASCADE"), nullable=False, index=True)
    
    link = Column(String(512), unique=True, nullable=False, index=True, comment="Ссылка на пост в Telegram") # Уникальность здесь может быть проблемой, если ссылка формируется не всегда 100% одинаково или может меняться. Обычно ID + Channel ID достаточно для уникальности.
    text_content = Column(Text, nullable=True, comment="Основное текстовое содержимое поста (если нет медиа, или если это не подпись к медиа)")
    views_count = Column(Integer, nullable=True, comment="Количество просмотров (если доступно)")
    comments_count = Column(Integer, default=0, nullable=False, comment="Количество комментариев к посту (обновляется сборщиком)")
    posted_at = Column(DateTime(timezone=True), nullable=False, comment="Время публикации поста в Telegram")
    
    summary_text = Column(Text, nullable=True, comment="Суммаризация поста (AI)")
    post_sentiment_label = Column(String(50), nullable=True, index=True, comment="Метка тональности текста поста") # Добавил index=True
    post_sentiment_score = Column(Float, nullable=True, comment="Числовая оценка тональности текста поста")

    # --- НОВЫЕ ПОЛЯ ДЛЯ РАСШИРЕННОГО СБОРА ДАННЫХ ---
    reactions = Column(JSONB, nullable=True, comment="Данные о реакциях на пост (список объектов ReactionCount)")
    # reactions_total_sum - было предложение добавить его в предыдущих шагах для сортировки, если оно есть - хорошо, если нет, можно добавить позже или считать на лету.
    # Если вы его добавили, оно должно быть здесь. Для примера ниже я его не добавляю, так как его не было в вашем исходном коде.
    
    media_type = Column(String(50), nullable=True, comment="Тип медиавложения (photo, video, poll, etc.)")
    media_content_info = Column(JSONB, nullable=True, comment="Дополнительная информация о медиа (URL, длительность, варианты опроса и т.д.)")
    caption_text = Column(Text, nullable=True, comment="Текст подписи к медиа (если есть медиа)")
    
    reply_to_telegram_post_id = Column(Integer, nullable=True, index=True, comment="ID поста в Telegram, на который этот пост является ответом")
    
    forwards_count = Column(Integer, nullable=True, comment="Количество пересылок поста")
    
    author_signature = Column(String(255), nullable=True, comment="Подпись автора поста (если есть, для каналов)")
    sender_user_id = Column(BigInteger, nullable=True, index=True, comment="Telegram ID пользователя-отправителя (если пост от имени пользователя, а не канала)")
    
    grouped_id = Column(BigInteger, nullable=True, index=True, comment="ID группы медиа (если пост часть альбома)")
    edited_at = Column(DateTime(timezone=True), nullable=True, comment="Время последнего редактирования поста")
    is_pinned = Column(Boolean, default=False, nullable=False, comment="Является ли пост закрепленным")
    # --- КОНЕЦ НОВЫХ ПОЛЕЙ ---

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="Время добавления в нашу БД")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    channel = relationship("Channel", back_populates="posts")
    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan", order_by="Comment.commented_at") # Добавил order_by для комментариев

    def __repr__(self):
        return f"<Post(id={self.id}, telegram_post_id={self.telegram_post_id}, channel_id={self.channel_id})>"

class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True, comment="Внутренний ID комментария")
    telegram_comment_id = Column(Integer, index=True, nullable=False, comment="ID комментария в Telegram")
    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False, index=True)
    
    telegram_user_id = Column(BigInteger, nullable=True, index=True, comment="Telegram ID пользователя-автора комментария")
    user_username = Column(String(255), nullable=True, comment="Username пользователя-автора")
    user_fullname = Column(String(255), nullable=True, comment="Полное имя пользователя-автора")
    
    text_content = Column(Text, nullable=True, comment="Текст комментария (может быть null, если только медиа)") # Изменил nullable=False на nullable=True
    commented_at = Column(DateTime(timezone=True), nullable=False, index=True, comment="Время публикации комментария") # Добавил index=True
    
    sentiment_score = Column(Float, nullable=True, comment="Оценка тональности (AI) комментария")
    sentiment_label = Column(String(50), nullable=True, index=True, comment="Метка тональности (AI) комментария") # Добавил index=True

    # --- НОВЫЕ ПОЛЯ ДЛЯ РАСШИРЕННОГО СБОРА ДАННЫХ (из вашего кода) ---
    reactions = Column(JSONB, nullable=True, comment="Данные о реакциях на комментарий")
    reply_to_telegram_comment_id = Column(Integer, nullable=True, index=True, comment="ID комментария в Telegram, на который этот комментарий является ответом")
    media_type = Column(String(50), nullable=True, comment="Тип медиавложения в комментарии")
    media_content_info = Column(JSONB, nullable=True, comment="Дополнительная информация о медиа в комментарии")
    caption_text = Column(Text, nullable=True, comment="Текст подписи к медиа в комментарии (если есть)")
    edited_at = Column(DateTime(timezone=True), nullable=True, comment="Время последнего редактирования комментария")
    # --- КОНЕЦ НОВЫХ ПОЛЕЙ (из вашего кода) ---

    # --- НАЧАЛО НОВЫХ ПОЛЕЙ ДЛЯ AI-АНАЛИЗА КОММЕНТАРИЕВ ---
    extracted_topics = Column(JSONB, nullable=True, comment="AI: Извлеченные темы/сущности из комментария")
    extracted_problems = Column(JSONB, nullable=True, comment="AI: Извлеченные проблемы/боли из комментария")
    extracted_questions = Column(JSONB, nullable=True, comment="AI: Извлеченные вопросы из комментария")
    extracted_suggestions = Column(JSONB, nullable=True, comment="AI: Извлеченные предложения/идеи из комментария")
    ai_analysis_completed_at = Column(DateTime(timezone=True), nullable=True, index=True, comment="AI: Время последнего анализа комментария") # Добавил index=True
    # --- КОНЕЦ НОВЫХ ПОЛЕЙ ---

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="Время добавления в нашу БД")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    post = relationship("Post", back_populates="comments")

    def __repr__(self):
        return f"<Comment(id={self.id}, telegram_comment_id={self.telegram_comment_id}, post_id={self.post_id})>"