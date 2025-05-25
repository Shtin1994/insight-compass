# --- START OF FILE app/celery_app.py (Updated with AI Summarization Schedule) ---

from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

REDIS_URL = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

# Важно: имя 'app.tasks' должно соответствовать тому, где Celery будет искать задачи.
# Если tasks.py лежит в app/tasks.py, то 'app.tasks' корректно.
celery_instance = Celery(
    'insight_compass_tasks', # Имя вашего Celery приложения
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['app.tasks'] # Список модулей, где определены задачи
)

# --- НАСТРОЙКА CELERY BEAT SCHEDULE ---
celery_instance.conf.beat_schedule = {
    'collect-telegram-data-every-10-minutes': {
        'task': 'collect_telegram_data', # Имя задачи, как оно определено в @celery_instance.task(name="...")
        'schedule': crontab(minute='*/10'),  # Запускать каждые 10 минут
    },
    # НОВАЯ ЗАДАЧА В РАСПИСАНИИ
    'summarize-top-posts-daily': {
        'task': 'summarize_top_posts', # Имя новой задачи из app/tasks.py
        'schedule': crontab(hour='3', minute='0'), # Запускать каждый день в 3:00 ночи (по времени UTC, если timezone UTC)
        # 'schedule': crontab(minute='*/15'), # Для теста можно запускать чаще, например, каждые 15 минут
        'args': (48, 3), # Аргументы для задачи: (hours_ago=48, top_n=3)
                         # Будут суммаризированы топ-3 поста за последние 48 часов.
    },
}

# Опционально: часовой пояс для Celery Beat
celery_instance.conf.timezone = 'UTC' # Рекомендуется использовать UTC для избежания путаницы

# Можно добавить другие конфигурации Celery здесь, если необходимо
# celery_instance.conf.update(
#    task_serializer='json',
#    accept_content=['json'],
#    result_serializer='json',
# )

if __name__ == '__main__':
    celery_instance.start()

# --- END OF FILE app/celery_app.py (Updated with AI Summarization Schedule) ---