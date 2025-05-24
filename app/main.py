from fastapi import FastAPI, Depends # Добавили Depends
from sqlalchemy.ext.asyncio import AsyncSession # Добавили AsyncSession
from sqlalchemy.sql import text # Добавили text для сырого SQL
from app.core.config import settings
from app.db.session import get_async_db, ASYNC_DATABASE_URL # Импортируем нашу зависимость и URL
# Добавляем новый таск в импорты из app.tasks
from app.tasks import simple_debug_task, add, collect_telegram_data_task 
app = FastAPI(title=settings.PROJECT_NAME)

@app.on_event("startup")
async def startup_event():
    print(f"FastAPI приложение запущено! DB URL: {settings.DATABASE_URL}")
    print(f"FastAPI Async DB URL: {ASYNC_DATABASE_URL}")


@app.get("/")
async def root():
    return {"message": f"Добро пожаловать в {settings.PROJECT_NAME}!"}

@app.get("/health")
async def health_check():
    return {"status": "OK", "project_name": settings.PROJECT_NAME}

@app.post("/collect-data-now/")
async def trigger_collect_data():
    """
    Запускает Celery таск для сбора данных из Telegram.
    """
    task_collect_async = collect_telegram_data_task.delay()
    return {
        "message": "Задача сбора данных запущена!",
        "task_id": task_collect_async.id
    }

# НОВЫЙ ЭНДПОИНТ ДЛЯ ПРОВЕРКИ БД
@app.get("/db-check")
async def db_check(db: AsyncSession = Depends(get_async_db)):
    try:
        # Выполняем простой запрос для проверки соединения
        result = await db.execute(text("SELECT 1"))
        value = result.scalar_one_or_none()
        if value == 1:
            return {"db_status": "OK", "message": "Успешное подключение к базе данных и выполнение запроса."}
        else:
            return {"db_status": "ERROR", "message": "Запрос 'SELECT 1' вернул не 1."}
    except Exception as e:
        return {"db_status": "ERROR", "message": f"Ошибка подключения к базе данных: {str(e)}"}


@app.post("/test-celery-task/")
async def test_celery(message: str): # Убрал BackgroundTasks, т.к. не используется
    task_result_async = simple_debug_task.delay(message)
    return {
        "message": "Celery задача отправлена!", 
        "task_info": f"Задача simple_debug_task с сообщением '{message}' поставлена в очередь.",
        "task_id": task_result_async.id
    }

@app.post("/test-add-task/")
async def test_add(x: int, y: int):
    task_add_result_async = add.delay(x, y)
    return {
        "message": "Задача сложения отправлена в Celery!",
        "task_id": task_add_result_async.id
    }

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
            response["error"] = str(task_result.info)
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.APP_HOST, port=settings.APP_PORT, reload=True)