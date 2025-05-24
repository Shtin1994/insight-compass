from app.celery_app import celery_instance # Импортируем наш экземпляр Celery
import time

@celery_instance.task(name="add_numbers") # Декоратор для объявления задачи, name - опциональное имя задачи
def add(x: int, y: int) -> int:
    print(f"Celery таск 'add_numbers': Получены числа {x} и {y}")
    time.sleep(5) # Имитируем длительную операцию
    result = x + y
    print(f"Celery таск 'add_numbers': Результат сложения {x} + {y} = {result}")
    return result

@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str):
    print(f"Сообщение от simple_debug_task: {message}")
    return f"Сообщение '{message}' обработано Celery!"

# Здесь в будущем будут наши задачи для сбора данных из Telegram