# --- START OF FILE app/main.py (Updated with Summarization Trigger) ---

from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from app.core.config import settings
from app.db.session import get_async_db, ASYNC_DATABASE_URL

# Обновляем импорт задач, добавляя summarize_top_posts_task
from app.tasks import (
    simple_debug_task, 
    add, 
    collect_telegram_data_task, 
    summarize_top_posts_task  # <--- НОВАЯ ЗАДАЧА ДОБАВЛЕНА В ИМПОРТ
)
from app.celery_app import celery_instance # Импортируем экземпляр Celery для проверки статуса задач

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API для Инсайт-Компаса - AI-помощника для анализа Telegram-каналов.",
    version="0.1.0" # Можно добавить версию API
)

@app.on_event("startup")
async def startup_event():
    print(f"FastAPI приложение '{settings.PROJECT_NAME}' запущено!")
    print(f"  Синхронный DB URL (используется Alembic): {settings.DATABASE_URL}")
    print(f"  Асинхронный DB URL (используется приложением): {ASYNC_DATABASE_URL}")
    print(f"  OpenAI API Key загружен: {'Да' if settings.OPENAI_API_KEY else 'Нет (проверьте .env)'}")


@app.get("/", tags=["Root"])
async def root():
    """
    Корневой эндпоинт. Возвращает приветственное сообщение.
    """
    return {"message": f"Добро пожаловать в {settings.PROJECT_NAME}!"}

@app.get("/health", tags=["Utilities"])
async def health_check():
    """
    Проверка работоспособности сервиса.
    """
    return {"status": "OK", "project_name": settings.PROJECT_NAME, "version": app.version}

@app.get("/db-check", tags=["Utilities"])
async def db_check(db: AsyncSession = Depends(get_async_db)):
    """
    Проверка соединения с базой данных.
    """
    try:
        result = await db.execute(text("SELECT 1"))
        value = result.scalar_one_or_none()
        if value == 1:
            return {"db_status": "OK", "message": "Успешное подключение к базе данных."}
        else:
            # Эта ветка маловероятна, если запрос SELECT 1 выполнился без ошибок
            return {"db_status": "ERROR", "message": "Запрос 'SELECT 1' не вернул ожидаемое значение."}
    except Exception as e:
        # Возвращаем 503 Service Unavailable, если БД недоступна
        # from fastapi import HTTPException, status
        # raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Ошибка подключения к базе данных: {str(e)}")
        return {"db_status": "ERROR", "message": f"Ошибка подключения к базе данных: {str(e)}"}


# --- Эндпоинты для запуска Celery задач ---

@app.post("/collect-data-now/", tags=["Data Collection Tasks"], status_code=202)
async def trigger_collect_data():
    """
    Асинхронно запускает Celery задачу для сбора данных из Telegram-каналов.
    Возвращает ID запущенной задачи.
    """
    task_collect_async = collect_telegram_data_task.delay()
    return {
        "message": "Задача сбора данных из Telegram запущена!",
        "task_id": task_collect_async.id
    }

# НОВЫЙ ЭНДПОИНТ ДЛЯ ЗАПУСКА СУММАРИЗАЦИИ
@app.post("/summarize-posts-now/", tags=["AI Analysis Tasks"], status_code=202)
async def trigger_summarize_posts(hours_ago: int = 48, top_n: int = 3):
    """
    Асинхронно запускает Celery задачу для AI суммаризации топ-N постов.
    
    - **hours_ago**: За какой период в часах назад от текущего времени выбирать посты.
    - **top_n**: Количество самых обсуждаемых постов для суммаризации.
    
    Возвращает ID запущенной задачи.
    """
    if not settings.OPENAI_API_KEY:
        # Можно добавить HTTPException, если ключ не настроен, или пусть задача сама это обработает
        from fastapi import HTTPException, status
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="OpenAI API Key не настроен. Суммаризация невозможна."
        )
        
    task_summarize_async = summarize_top_posts_task.delay(hours_ago=hours_ago, top_n=top_n)
    return {
        "message": f"Задача AI суммаризации топ-{top_n} постов за последние {hours_ago}ч запущена!",
        "task_id": task_summarize_async.id
    }

# --- Тестовые эндпоинты для Celery (можно оставить для отладки) ---

@app.post("/test-celery-debug-task/", tags=["Test Celery Tasks"], status_code=202)
async def test_celery_debug(message: str = "Привет от FastAPI!"):
    """
    Запускает тестовую отладочную Celery задачу.
    """
    task_result_async = simple_debug_task.delay(message)
    return {
        "message": "Тестовая отладочная Celery задача отправлена!",
        "task_info": f"Задача simple_debug_task с сообщением '{message}' поставлена в очередь.",
        "task_id": task_result_async.id
    }

@app.post("/test-celery-add-task/", tags=["Test Celery Tasks"], status_code=202)
async def test_celery_add(x: int = 5, y: int = 7):
    """
    Запускает тестовую задачу сложения в Celery.
    """
    task_add_result_async = add.delay(x, y)
    return {
        "message": "Задача сложения отправлена в Celery!",
        "task_id": task_add_result_async.id
    }

@app.get("/task-status/{task_id}", tags=["Utilities"])
async def get_task_status(task_id: str):
    """
    Получает статус выполнения Celery задачи по её ID.
    """
    task_result = celery_instance.AsyncResult(task_id)
    
    response = {
        "task_id": task_id,
        "status": task_result.status, # PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED
        "ready": task_result.ready(), # True, если задача завершена (SUCCESS или FAILURE)
    }
    if task_result.ready():
        if task_result.successful():
            response["result"] = task_result.get()
        elif task_result.failed():
            # task_result.info может содержать исключение или трейсбек
            # task_result.traceback содержит форматированный трейсбек
            try:
                # Пытаемся получить результат, который может быть исключением
                task_result.get(propagate=False) 
                response["error_info"] = str(task_result.info) if task_result.info else "Неизвестная ошибка"
                response["traceback"] = task_result.traceback
            except Exception as e:
                response["error_info"] = f"Исключение при получении результата задачи: {str(e)}"
                response["traceback"] = task_result.traceback
        # Для статуса REVOKED (отозвана) и других специфичных можно добавить обработку
    elif task_result.status == 'STARTED':
        response["meta"] = task_result.info # Может содержать текущий прогресс, если задача его обновляет
        
    return response

# Этот блок нужен только если вы запускаете main.py напрямую через `python app/main.py`
# При запуске через Uvicorn из Dockerfile он не используется.
if __name__ == "__main__":
    import uvicorn
    # Загружаем настройки хоста и порта из конфигурации
    uvicorn.run(
        "main:app", 
        host=settings.APP_HOST, 
        port=settings.APP_PORT, 
        reload=True, # reload=True удобно для разработки, отключается в продакшене
        log_level="info" # Можно установить 'debug' для более подробных логов uvicorn
    )

# --- END OF FILE app/main.py (Updated with Summarization Trigger) ---