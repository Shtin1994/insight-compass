from fastapi import FastAPI
from core.config import settings # Импортируем наши настройки <--- ИЗМЕНЕНО ЗДЕСЬ

app = FastAPI(title=settings.PROJECT_NAME)

@app.get("/")
async def root():
    return {"message": f"Добро пожаловать в {settings.PROJECT_NAME}!"}

@app.get("/health")
async def health_check():
    return {"status": "OK", "project_name": settings.PROJECT_NAME}

# Для запуска uvicorn напрямую (если не через Docker)
# Этот блок if __name__ == "__main__": может потребовать другого подхода к импорту,
# если вы будете запускать main.py напрямую из командной строки вне Docker,
# но для Docker-запуска это не повлияет. Пока оставим так.
if __name__ == "__main__":
    import uvicorn
    # Для локального запуска вне Docker, PYTHONPATH может потребовать настройки
    # или запуск uvicorn из корневой папки проекта: uvicorn app.main:app --reload
    uvicorn.run("main:app", host=settings.APP_HOST, port=settings.APP_PORT, reload=True) # Уточнил вызов uvicorn для этого случая