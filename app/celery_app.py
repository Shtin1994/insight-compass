# app/celery_app.py

from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

REDIS_URL = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

celery_instance = Celery(
    'insight_compass_tasks', # Используем то же имя приложения, что и в tasks.py
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['app.tasks'] 
)

celery_instance.conf.beat_schedule = {
    'collect-telegram-data-every-10-minutes': {
        'task': 'collect_telegram_data', 
        'schedule': crontab(minute='*/10'), # Каждые 10 минут
    },
    'summarize-top-posts-daily': {
        'task': 'summarize_top_posts', 
        'schedule': crontab(hour='3', minute='0'), 
        'args': (48, 3), 
    },
    'send-daily-digest-via-telegram': {
        'task': 'send_daily_digest', 
        'schedule': crontab(hour='3', minute='30'), 
        'args': (24, 3),
    },
    # --- НОВАЯ ЗАДАЧА В РАСПИСАНИИ ДЛЯ АНАЛИЗА ТОНАЛЬНОСТИ ---
    'analyze-posts-sentiment-periodically': {
        'task': 'analyze_posts_sentiment', # Имя новой задачи из app.tasks
        'schedule': crontab(minute='*/15'),  # Например, каждые 15 минут
        'args': (10,), # Аргументы для задачи: (limit_posts_to_analyze=10)
                       # Будет анализировать до 10 постов за запуск.
    },
    # --- КОНЕЦ НОВОЙ ЗАДАЧИ В РАСПИСАНИИ ---
}

celery_instance.conf.timezone = 'UTC'

# Опциональные глобальные настройки для всех задач, если еще не заданы
celery_instance.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1, # Для одного воркера с concurrency=1
)


if __name__ == '__main__':
    celery_instance.start()