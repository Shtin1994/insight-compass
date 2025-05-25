# --- START OF FILE app/main.py (Updated with Digest Trigger) ---

from fastapi import FastAPI, Depends, HTTPException, status # Добавил HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from app.core.config import settings
from app.db.session import get_async_db, ASYNC_DATABASE_URL

# Обновляем импорт задач, добавляя send_daily_digest_task
from app.tasks import (
    simple_debug_task, 
    add, 
    collect_telegram_data_task, 
    summarize_top_posts_task,
    send_daily_digest_task # <--- НОВАЯ ЗАДАЧА ДАЙДЖЕСТА ДОБАВЛЕНА В ИМПОРТ
)
from app.celery_app import celery_instance

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API для Инсайт-Компаса - AI-помощника для анализа Telegram-каналов.",
    version="0.1.0"
)

@app.on_event("startup")
async def startup_event():
    print(f"FastAPI приложение '{settings.PROJECT_NAME}' запущено!")
    print(f"  Синхронный DB URL (используется Alembic): {settings.DATABASE_URL}")
    print(f"  Асинхронный DB URL (используется приложением): {ASYNC_DATABASE_URL}")
    print(f"  OpenAI API Key загружен: {'Да' if settings.OPENAI_API_KEY else 'Нет (проверьте .env)'}")
    print(f"  Telegram Bot Token загружен: {'Да' if settings.TELEGRAM_BOT_TOKEN else 'Нет (проверьте .env)'}")
    print(f"  Telegram Target Chat ID: {settings.TELEGRAM_TARGET_CHAT_ID if settings.TELEGRAM_TARGET_CHAT_ID else 'Не указан (проверьте .env)'}")


@app.get("/", tags=["Root"])
async def root():
    return {"message": f"Добро пожаловать в {settings.PROJECT_NAME}!"}

@app.get("/health", tags=["Utilities"])
async def health_check():
    return {"status": "OK", "project_name": settings.PROJECT_NAME, "version": app.version}

@app.get("/db-check", tags=["Utilities"])
async def db_check(db: AsyncSession = Depends(get_async_db)):
    try:
        result = await db.execute(text("SELECT 1"))
        value = result.scalar_one_or_none()
        if value == 1:
            return {"db_status": "OK", "message": "Успешное подключение к базе данных."}
        else:
            return {"db_status": "ERROR", "message": "Запрос 'SELECT 1' не вернул ожидаемое значение."}
    except Exception as e:
        return {"db_status": "ERROR", "message": f"Ошибка подключения к базе данных: {str(e)}"}


# --- Эндпоинты для запуска Celery задач ---

@app.post("/collect-data-now/", tags=["Data Collection Tasks"], status_code=status.HTTP_202_ACCEPTED)
async def trigger_collect_data():
    task_collect_async = collect_telegram_data_task.delay()
    return {
        "message": "Задача сбора данных из Telegram запущена!",
        "task_id": task_collect_async.id
    }

@app.post("/summarize-posts-now/", tags=["AI Analysis Tasks"], status_code=status.HTTP_202_ACCEPTED)
async def trigger_summarize_posts(hours_ago: int = 48, top_n: int = 3):
    if not settings.OPENAI_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="OpenAI API Key не настроен. Суммаризация невозможна."
        )
    task_summarize_async = summarize_top_posts_task.delay(hours_ago=hours_ago, top_n=top_n)
    return {
        "message": f"Задача AI суммаризации топ-{top_n} постов за последние {hours_ago}ч запущена!",
        "task_id": task_summarize_async.id
    }

# НОВЫЙ ЭНДПОИНТ ДЛЯ ЗАПУСКА ОТПРАВКИ ДАЙДЖЕСТА
@app.post("/send-digest-now/", tags=["Digest Tasks"], status_code=status.HTTP_202_ACCEPTED)
async def trigger_send_digest(hours_ago_posts: int = 24, top_n_summarized: int = 3):
    """
    Асинхронно запускает Celery задачу для формирования и отправки Telegram-дайджеста.
    
    - **hours_ago_posts**: За какой период в часах назад от текущего времени выбирать посты для статистики и топа.
    - **top_n_summarized**: Количество самых обсуждаемых постов для включения в дайджест.
    
    Возвращает ID запущенной задачи.
    """
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_TARGET_CHAT_ID:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail="TELEGRAM_BOT_TOKEN или TELEGRAM_TARGET_CHAT_ID не настроены. Отправка дайджеста невозможна."
        )
        
    # Используем .send_task для асинхронных задач Celery, если хотим быть уверены,
    # что они будут выполнены асинхронно, даже если сама задача определена как async def.
    # .delay() тоже должен работать, но send_task более явный для async задач.
    # В данном случае, так как send_daily_digest_task уже async, .delay() будет работать корректно.
    task_digest_async = send_daily_digest_task.delay(hours_ago_posts=hours_ago_posts, top_n_summarized=top_n_summarized)
    # Альтернативно:
    # task_digest_async = celery_instance.send_task(
    #     "send_daily_digest", 
    #     args=[hours_ago_posts, top_n_summarized]
    # )
    
    return {
        "message": f"Задача отправки Telegram-дайджеста (посты за {hours_ago_posts}ч, топ-{top_n_summarized}) запущена!",
        "task_id": task_digest_async.id
    }

# --- Тестовые эндпоинты для Celery ---

@app.post("/test-celery-debug-task/", tags=["Test Celery Tasks"], status_code=status.HTTP_202_ACCEPTED)
async def test_celery_debug(message: str = "Привет от FastAPI!"):
    task_result_async = simple_debug_task.delay(message)
    return {
        "message": "Тестовая отладочная Celery задача отправлена!",
        "task_info": f"Задача simple_debug_task с сообщением '{message}' поставлена в очередь.",
        "task_id": task_result_async.id
    }

@app.post("/test-celery-add-task/", tags=["Test Celery Tasks"], status_code=status.HTTP_202_ACCEPTED)
async def test_celery_add(x: int = 5, y: int = 7):
    task_add_result_async = add.delay(x, y)
    return {
        "message": "Задача сложения отправлена в Celery!",
        "task_id": task_add_result_async.id
    }

@app.get("/task-status/{task_id}", tags=["Utilities"])
async def get_task_status_endpoint(task_id: str): # Переименовал для избежания конфликта имен
    task_result = celery_instance.AsyncResult(task_id)
    
    response = {
        "task_id": task_id,
        "status": task_result.status,
        "ready": task_result.ready(),
    }
    if task_result.ready():
        if task_result.successful():
            response["result"] = task_result.get()
        elif task_result.failed():
            try:
                task_result.get(propagate=False) 
                response["error_info"] = str(task_result.info) if task_result.info else "Неизвестная ошибка"
                response["traceback"] = task_result.traceback
            except Exception as e:
                response["error_info"] = f"Исключение при получении результата задачи: {str(e)}"
                response["traceback"] = task_result.traceback
    elif task_result.status == 'STARTED':
        response["meta"] = task_result.info 
        
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app", 
        host=settings.APP_HOST, 
        port=settings.APP_PORT, 
        reload=True,
        log_level="info"
    )

# --- END OF FILE app/main.py (Updated with Digest Trigger) ---