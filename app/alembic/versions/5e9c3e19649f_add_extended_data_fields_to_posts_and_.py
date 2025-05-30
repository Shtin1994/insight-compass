"""add_extended_data_fields_to_posts_and_comments

Revision ID: 5e9c3e19649f
Revises: 309b21a03da8
Create Date: 2025-05-31 06:11:11.865139

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql # <--- ВАЖНО: импортировать для JSONB

# revision identifiers, used by Alembic.
revision: str = '5e9c3e19649f'
down_revision: Union[str, None] = '309b21a03da8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # Добавляем новые поля в таблицу 'posts'
    op.add_column('posts', sa.Column('reactions', postgresql.JSONB(astext_type=sa.Text()), nullable=True, comment="Данные о реакциях на пост (список объектов ReactionCount)"))
    op.add_column('posts', sa.Column('media_type', sa.String(length=50), nullable=True, comment="Тип медиавложения (photo, video, poll, etc.)"))
    op.add_column('posts', sa.Column('media_content_info', postgresql.JSONB(astext_type=sa.Text()), nullable=True, comment="Дополнительная информация о медиа (URL, длительность, варианты опроса и т.д.)"))
    op.add_column('posts', sa.Column('caption_text', sa.Text(), nullable=True, comment="Текст подписи к медиа (если есть медиа)"))
    op.add_column('posts', sa.Column('reply_to_telegram_post_id', sa.Integer(), nullable=True, comment="ID поста в Telegram, на который этот пост является ответом"))
    op.create_index(op.f('ix_posts_reply_to_telegram_post_id'), 'posts', ['reply_to_telegram_post_id'], unique=False) # Индекс для reply_to
    op.add_column('posts', sa.Column('forwards_count', sa.Integer(), nullable=True, comment="Количество пересылок поста"))
    op.add_column('posts', sa.Column('author_signature', sa.String(length=255), nullable=True, comment="Подпись автора поста (если есть, для каналов)"))
    op.add_column('posts', sa.Column('sender_user_id', sa.BigInteger(), nullable=True, comment="Telegram ID пользователя-отправителя (если пост от имени пользователя, а не канала)"))
    op.create_index(op.f('ix_posts_sender_user_id'), 'posts', ['sender_user_id'], unique=False) # Индекс для sender_user_id
    op.add_column('posts', sa.Column('grouped_id', sa.BigInteger(), nullable=True, comment="ID группы медиа (если пост часть альбома)"))
    op.create_index(op.f('ix_posts_grouped_id'), 'posts', ['grouped_id'], unique=False) # Индекс для grouped_id
    op.add_column('posts', sa.Column('edited_at', sa.DateTime(timezone=True), nullable=True, comment="Время последнего редактирования поста"))
    op.add_column('posts', sa.Column('is_pinned', sa.Boolean(), nullable=False, server_default=sa.text('false'), comment="Является ли пост закрепленным"))


    # Добавляем новые поля в таблицу 'comments'
    op.add_column('comments', sa.Column('reactions', postgresql.JSONB(astext_type=sa.Text()), nullable=True, comment="Данные о реакциях на комментарий"))
    op.add_column('comments', sa.Column('reply_to_telegram_comment_id', sa.Integer(), nullable=True, comment="ID комментария в Telegram, на который этот комментарий является ответом"))
    op.create_index(op.f('ix_comments_reply_to_telegram_comment_id'), 'comments', ['reply_to_telegram_comment_id'], unique=False) # Индекс для reply_to
    op.add_column('comments', sa.Column('media_type', sa.String(length=50), nullable=True, comment="Тип медиавложения в комментарии"))
    op.add_column('comments', sa.Column('media_content_info', postgresql.JSONB(astext_type=sa.Text()), nullable=True, comment="Дополнительная информация о медиа в комментарии"))
    op.add_column('comments', sa.Column('caption_text', sa.Text(), nullable=True, comment="Текст подписи к медиа в комментарии (если есть)"))
    op.add_column('comments', sa.Column('edited_at', sa.DateTime(timezone=True), nullable=True, comment="Время последнего редактирования комментария"))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # Удаляем поля из таблицы 'comments' (в обратном порядке добавления)
    op.drop_column('comments', 'edited_at')
    op.drop_column('comments', 'caption_text')
    op.drop_column('comments', 'media_content_info')
    op.drop_column('comments', 'media_type')
    op.drop_index(op.f('ix_comments_reply_to_telegram_comment_id'), table_name='comments')
    op.drop_column('comments', 'reply_to_telegram_comment_id')
    op.drop_column('comments', 'reactions')

    # Удаляем поля из таблицы 'posts' (в обратном порядке добавления)
    op.drop_column('posts', 'is_pinned')
    op.drop_column('posts', 'edited_at')
    op.drop_index(op.f('ix_posts_grouped_id'), table_name='posts')
    op.drop_column('posts', 'grouped_id')
    op.drop_index(op.f('ix_posts_sender_user_id'), table_name='posts')
    op.drop_column('posts', 'sender_user_id')
    op.drop_column('posts', 'author_signature')
    op.drop_column('posts', 'forwards_count')
    op.drop_index(op.f('ix_posts_reply_to_telegram_post_id'), table_name='posts')
    op.drop_column('posts', 'reply_to_telegram_post_id')
    op.drop_column('posts', 'caption_text')
    op.drop_column('posts', 'media_content_info')
    op.drop_column('posts', 'media_type')
    op.drop_column('posts', 'reactions')
    # ### end Alembic commands ###