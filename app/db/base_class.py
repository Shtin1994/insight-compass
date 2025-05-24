from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData

# Соглашение об именовании для индексов и ограничений внешних ключей
# https://alembic.sqlalchemy.org/en/latest/naming.html
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=convention)

class Base(DeclarativeBase):
    metadata = metadata
    # Здесь можно определить общие поля для всех таблиц, например, id
    # Или __tablename__ генератор, если нужно