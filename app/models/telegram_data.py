# app/models/telegram_data.py
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, BigInteger, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func 
from app.db.base_class import Base 

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
    link = Column(String(512), unique=True, nullable=False, index=True, comment="Ссылка на пост в Telegram")
    text_content = Column(Text, nullable=True, comment="Текстовое содержимое поста") # Используем это имя для текста поста
    views_count = Column(Integer, nullable=True, comment="Количество просмотров (если доступно)")
    comments_count = Column(Integer, default=0, nullable=False, comment="Количество комментариев к посту")
    posted_at = Column(DateTime(timezone=True), nullable=False, comment="Время публикации поста в Telegram")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="Время добавления в нашу БД")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    summary_text = Column(Text, nullable=True, comment="Суммаризация поста (AI)")

    # --- НОВЫЕ ПОЛЯ ДЛЯ АНАЛИЗА ТОНАЛЬНОСТИ ТЕКСТА ПОСТА ---
    post_sentiment_label = Column(String(50), nullable=True, comment="Метка тональности текста поста (e.g., positive, negative, neutral)")
    post_sentiment_score = Column(Float, nullable=True, comment="Числовая оценка тональности текста поста (e.g., от -1 до 1, или вероятность)")
    # --- КОНЕЦ НОВЫХ ПОЛЕЙ ---

    channel = relationship("Channel", back_populates="posts")
    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Post(id={self.id}, telegram_post_id={self.telegram_post_id}, channel_id={self.channel_id})>"

class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True, comment="Внутренний автоинкрементный ID комментария")
    telegram_comment_id = Column(Integer, index=True, nullable=False, comment="ID комментария в Telegram")
    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), nullable=False, index=True)
    telegram_user_id = Column(BigInteger, nullable=True, index=True, comment="Telegram ID пользователя")
    user_username = Column(String(255), nullable=True, comment="Username пользователя")
    user_fullname = Column(String(255), nullable=True, comment="Полное имя пользователя")
    text_content = Column(Text, nullable=False, comment="Текст комментария") # Используем это имя для текста комментария
    commented_at = Column(DateTime(timezone=True), nullable=False, comment="Время публикации комментария в Telegram")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="Время добавления в нашу БД")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    sentiment_score = Column(Float, nullable=True, comment="Оценка тональности (AI) комментария") # Уточнил комментарий
    sentiment_label = Column(String(50), nullable=True, comment="Метка тональности (AI) комментария") # Уточнил комментарий

    post = relationship("Post", back_populates="comments")

    def __repr__(self):
        return f"<Comment(id={self.id}, telegram_comment_id={self.telegram_comment_id}, post_id={self.post_id})>"