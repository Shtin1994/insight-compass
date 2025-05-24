# Этот файл служит для удобного импорта Base и всех моделей в одном месте,
# особенно полезно для Alembic env.py и для инициализации БД.

from app.db.base_class import Base # Наш DeclarativeMeta
from app.models.telegram_data import Channel, Post, Comment # Импортируем наши модели