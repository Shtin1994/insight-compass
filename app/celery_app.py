from celery import Celery
from app.core.config import settings # Импортируем наши настройки приложения

# Формируем URL для Redis брокера из наших настроек
# Celery ожидает URL в формате: redis://host:port/db_number
# Мы будем использовать базу данных Redis номер 0 по умолчанию.
REDIS_URL = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

# Создаем экземпляр Celery
# 'tasks' - это имя текущего модуля или имя, которое будет использоваться для именования задач
# broker - URL нашего Redis брокера
# backend - URL для хранения результатов задач (также будем использовать Redis)
# include - список модулей, где Celery будет искать задачи (мы создадим tasks.py позже)
celery_instance = Celery(
    'insight_compass_tasks', # Можно дать любое осмысленное имя
    broker=REDIS_URL,
    backend=REDIS_URL, # Используем тот же Redis для хранения результатов
    include=['app.tasks'] # Указываем, где искать наши задачи
)

# Опционально: Конфигурация Celery из объекта настроек (если понадобится больше настроек)
# celery_instance.conf.update(
#     task_serializer='json',
#     result_serializer='json',
#     accept_content=['json'],
#     timezone='Europe/Moscow', # Пример: установите ваш часовой пояс
#     enable_utc=True,
# )

# Опционально: для отладки можно вывести конфигурацию
# print(f"Celery broker URL: {celery_instance.conf.broker_url}")
# print(f"Celery backend URL: {celery_instance.conf.result_backend}")

if __name__ == '__main__':
    # Эта часть нужна, чтобы можно было запустить Celery worker'а напрямую из командной строки
    # командой: celery -A app.celery_app worker -l info
    # (app.celery_app - путь к этому файлу и экземпляру celery_instance)
    celery_instance.start()