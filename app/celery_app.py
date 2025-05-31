# app/celery_app.py

from celery import Celery
from celery.schedules import crontab # crontab все еще импортируется, но не используется
from app.core.config import settings

REDIS_URL = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

celery_instance = Celery(
    'insight_compass_tasks',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['app.tasks']
)

# --- ОТКЛЮЧЕНО ВСЕ ПЕРИОДИЧЕСКОЕ РАСПИСАНИЕ ДЛЯ ТЕСТИРОВАНИЯ ---
# Задачи будут запускаться только вручную через API или прямым вызовом .delay()
celery_instance.conf.beat_schedule = {}

# Опционально: часовой пояс для Celery Beat (хотя beat сейчас неактивен)
celery_instance.conf.timezone = 'UTC'

# Другие конфигурации Celery
celery_instance.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_track_started=True, # Позволяет отслеживать, что задача начала выполняться
    task_acks_late=True,     # Подтверждение задачи после выполнения (а не при получении)
    worker_prefetch_multiplier=1, # Каждый воркер берет по одной задаче за раз
)

if __name__ == '__main__':
    celery_instance.start()