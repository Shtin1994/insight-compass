# В САМОМ НАЧАЛЕ ФАЙЛА
import os
import sys

# Добавляем корневую директорию проекта (где лежит папка 'app') в sys.path
# __file__ это /app/alembic/env.py
# os.path.dirname(__file__) это /app/alembic
# PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) -> /app
# ROOT_DIR = os.path.abspath(os.path.join(PROJECT_DIR, '..')) -> /
# Нам нужно, чтобы / (корень, где лежит пакет 'app') был в sys.path
# или сама папка 'app' (/app) была в sys.path, если мы импортируем как 'core.config'
# Давайте добавим /app в sys.path, если alembic запускается из /app

# Текущая рабочая директория для alembic, когда мы делаем `docker compose exec -w /app ...`, будет /app
# Чтобы импортировать `from app.db.base import Base`, Python должен "видеть" директорию,
# содержащую пакет `app`. Если `PYTHONPATH=/app` не сработал для `exec`,
# то `sys.path` внутри этого скрипта env.py может не содержать `/`.
# Давайте добавим / в sys.path, чтобы `app` был виден как пакет верхнего уровня.
current_script_path = os.path.dirname(os.path.abspath(__file__)) # /app/alembic
app_dir = os.path.abspath(os.path.join(current_script_path, '..')) # /app
project_root_dir = os.path.abspath(os.path.join(app_dir, '..')) # /

if project_root_dir not in sys.path:
    sys.path.insert(0, project_root_dir)
# Также добавим /app, на всякий случай, если импорты внутри app не будут разрешаться
if app_dir not in sys.path:
    sys.path.insert(1, app_dir) # Добавляем после project_root_dir

# Теперь остальные импорты
from logging.config import fileConfig
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

# Импортируем Base из нашего проекта и объект настроек
from app.db.base import Base
from app.core.config import settings

# это объект Alembic Config, который предоставляет доступ к
# значениям из .ini файла.
config = context.config

# Интерпретируем ini-файл для Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Устанавливаем URL базы данных из наших настроек приложения
config.set_main_option('sqlalchemy.url', settings.DATABASE_URL)

# target_metadata для операций 'autogenerate'
target_metadata = Base.metadata

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()