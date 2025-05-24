from fastapi import FastAPI, BackgroundTasks
from app.core.config import settings
from app.tasks import simple_debug_task, add # Импортируем наши задачи из tasks.py

app = FastAPI(title=settings.PROJECT_NAME)

@app.get("/")
async def root():
    return {"message": f"Добро пожаловать в {settings.PROJECT_NAME}!"}

@app.get("/health")
async def health_check():
    return {"status": "OK", "project_name": settings.PROJECT_NAME}

# НОВЫЙ ЭНДПОИНТ для тестирования Celery задачи
@app.post("/test-celery-task/")
async def test_celery(message: str, background_tasks: BackgroundTasks):
    """
    Тестовый эндпоинт для отправки задачи Celery.
    Использует BackgroundTasks для немедленного ответа, пока задача обрабатывается в фоне.
    """
    # Отправляем задачу в Celery.
    # .delay() - это шорткат для .send_task() с текущими настройками.
    task_result_async = simple_debug_task.delay(message)
    
    # Для получения результата задачи (если нужно и если backend настроен):
    # task_id = task_result_async.id
    # print(f"Задача simple_debug_task отправлена с ID: {task_id}")
    # Через некоторое время можно будет проверить результат по ID:
    # from app.celery_app import celery_instance
    # result = celery_instance.AsyncResult(task_id)
    # if result.ready():
    #     print(f"Результат задачи {task_id}: {result.get(timeout=1)}")
    # else:
    #     print(f"Задача {task_id} еще не выполнена, статус: {result.status}")

    # Мы также можем использовать FastAPI BackgroundTasks для задач, которые не требуют
    # отдельного worker'а, но для Celery это не нужно.
    # Здесь background_tasks не используется, но оставлен для примера.
    
    return {
        "message": "Celery задача отправлена!", 
        "task_info": f"Задача simple_debug_task с сообщением '{message}' поставлена в очередь."
        # "task_id": task_id # Можно вернуть ID задачи, если нужно
    }

@app.post("/test-add-task/")
async def test_add(x: int, y: int):
    """
    Тестовый эндпоинт для отправки задачи сложения в Celery.
    """
    task_add_result_async = add.delay(x, y)
    return {
        "message": "Задача сложения отправлена в Celery!",
        "task_id": task_add_result_async.id # Возвращаем ID задачи, чтобы потом можно было проверить результат
    }

# Опционально: эндпоинт для проверки статуса задачи
@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    from app.celery_app import celery_instance
    task_result = celery_instance.AsyncResult(task_id)
    
    response = {
        "task_id": task_id,
        "status": task_result.status,
        "ready": task_result.ready(),
    }
    if task_result.ready():
        if task_result.successful():
            response["result"] = task_result.get()
        else:
            response["error"] = str(task_result.info) # или task_result.traceback
    return response


# Для запуска uvicorn напрямую (если не через Docker)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.APP_HOST, port=settings.APP_PORT, reload=True)