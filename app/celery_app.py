# --- START OF FILE app/celery_app.py (Updated with Digest Schedule) ---

from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

REDIS_URL = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

celery_instance = Celery(
    'insight_compass_tasks',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['app.tasks'] 
)

celery_instance.conf.beat_schedule = {
    'collect-telegram-data-every-10-minutes': {
        'task': 'collect_telegram_data', 
        'schedule': crontab(minute='*/10'),
    },
    'summarize-top-posts-daily': {
        'task': 'summarize_top_posts', 
        'schedule': crontab(hour='3', minute='0'), # Ежедневно в 3:00 UTC
        # 'schedule': crontab(minute='*/20'), # Для теста - каждые 20 минут
        'args': (48, 3), 
    },
    # НОВАЯ ЗАДАЧА ДАЙДЖЕСТА В РАСПИСАНИИ
    'send-daily-digest-via-telegram': {
        'task': 'send_daily_digest', # Имя новой задачи из app/tasks.py
        'schedule': crontab(hour='3', minute='30'), # Ежедневно в 3:30 UTC (после суммаризации)
        # 'schedule': crontab(minute='*/25'), # Для теста - каждые 25 минут
        'args': (24, 3), # Аргументы: (hours_ago_posts=24, top_n_summarized=3)
                         # Дайджест за последние 24 часа, топ-3 поста.
    },
}

celery_instance.conf.timezone = 'UTC'

if __name__ == '__main__':
    celery_instance.start()

# --- END OF FILE app/celery_app.py (Updated with Digest Schedule) ---