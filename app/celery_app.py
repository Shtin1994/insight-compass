from celery import Celery
from celery.schedules import crontab # Для расписаний на основе cron
from app.core.config import settings

REDIS_URL = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

celery_instance = Celery(
    'insight_compass_tasks',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['app.tasks'] 
)

# --- НАСТРОЙКА CELERY BEAT SCHEDULE ---
celery_instance.conf.beat_schedule = {
    'collect-telegram-data-every-10-minutes': { # Уникальное имя для этой записи расписания
        'task': 'collect_telegram_data', # <--- ИЗМЕНЕНО ЗДЕСЬ! Используем имя из декоратора
        'schedule': crontab(minute='*/10'),  # Запускать каждые 10 минут
        # 'schedule': 60.0 * 1, # Или так: запускать каждые 60 секунд (1 минута) - для теста
        # 'args': (16, 16), # Аргументы для таска, если нужны (у нашего таска нет аргументов)
    },
    # Можно добавить другие периодические задачи здесь
    # 'test-simple-debug-every-minute': {
    #     'task': 'app.tasks.simple_debug_task',
    #     'schedule': 60.0, # Каждые 60 секунд
    #     'args': ("Сообщение от Celery Beat!",)
    # }
}

# Опционально: часовой пояс для Celery Beat
celery_instance.conf.timezone = 'UTC' # Рекомендуется использовать UTC

if __name__ == '__main__':
    celery_instance.start()