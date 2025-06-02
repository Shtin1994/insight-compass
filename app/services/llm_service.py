# app/services/llm_service.py

import httpx
import json # Импортируем json для возможной обработки ошибок
import logging
from typing import Optional, Dict, Any

from app.core.config import settings # Импортируем ваши настройки

# Настройка логгера
logger = logging.getLogger(__name__)
if not logger.handlers: # Чтобы избежать дублирования логов при перезагрузке в FastAPI
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


async def одиночный_запрос_к_llm(
    prompt_text: str,
    модель: Optional[str] = None,
    температура: float = 0.2, # Более низкая температура для предсказуемого JSON
    макс_токены: int = 350,    # Достаточно для JSON с извлеченными данными
    is_json_response_expected: bool = True # Флаг, ожидаем ли мы JSON
) -> Optional[str]:
    """
    Выполняет одиночный асинхронный запрос к API OpenAI (или совместимому).
    Возвращает строковый ответ от LLM или None в случае ошибки.
    """
    if not settings.OPENAI_API_KEY:
        logger.error("Ключ OpenAI API не настроен (OPENAI_API_KEY).")
        return None

    api_key = settings.OPENAI_API_KEY
    # Используем модель по умолчанию для задач, если конкретная не передана,
    # или основную модель по умолчанию, если и та не задана.
    target_model = модель or settings.OPENAI_DEFAULT_MODEL_FOR_TASKS or settings.OPENAI_DEFAULT_MODEL

    if not target_model:
        logger.error("Модель LLM не указана и не задана в настройках (OPENAI_DEFAULT_MODEL_FOR_TASKS или OPENAI_DEFAULT_MODEL).")
        return None

    # URL API, по умолчанию OpenAI, но можно переопределить в настройках для других провайдеров
    api_url = settings.OPENAI_API_URL or "https://api.openai.com/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "model": target_model,
        "messages": [{"role": "user", "content": prompt_text}],
        "temperature": температура,
        "max_tokens": макс_токены,
    }

    if is_json_response_expected:
        # Пытаемся включить режим JSON ответа, если модель его поддерживает (например, gpt-3.5-turbo-1106 и новее)
        # Некоторые модели могут не поддерживать этот параметр или требовать его в другом формате.
        # Для OpenAI это выглядит так:
        if "gpt-3.5-turbo-1106" in target_model or "gpt-4" in target_model: # Примерные проверки
            payload["response_format"] = {"type": "json_object"}
        else:
            logger.warning(f"Модель {target_model} может не поддерживать 'response_format': {{'type': 'json_object'}}. LLM может вернуть JSON не идеально.")


    logger.info(f"Отправка запроса к LLM: Модель='{target_model}', URL='{api_url}', JSON_ожидается={is_json_response_expected}, Макс.токены={макс_токены}, Температура={температура}")
    # Логгируем только часть промпта для безопасности и краткости
    logger.debug(f"Промпт (начало): {prompt_text[:200]}...") 

    try:
        async with httpx.AsyncClient(timeout=settings.OPENAI_TIMEOUT_SECONDS or 60.0) as client:
            response = await client.post(api_url, headers=headers, json=payload)
        
        response.raise_for_status() # Вызовет исключение для 4xx/5xx HTTP статусов
        
        response_data = response.json()
        logger.debug(f"Полный ответ от LLM: {response_data}")

        if response_data.get("choices") and len(response_data["choices"]) > 0:
            message = response_data["choices"][0].get("message", {})
            message_content = message.get("content")
            
            if message_content:
                logger.info(f"Ответ от LLM ({target_model}) успешно получен.")
                return message_content.strip()
            else:
                logger.warning(f"Ответ от LLM ({target_model}): ключ 'content' отсутствует в 'message'. Ответ: {response_data}")
                return None
        else:
            logger.warning(f"Ответ от LLM ({target_model}): массив 'choices' пуст или отсутствует. Ответ: {response_data}")
            return None

    except httpx.HTTPStatusError as e:
        error_body = "Не удалось прочитать тело ошибки."
        try:
            error_body = e.response.text
        except Exception:
            pass
        logger.error(f"Ошибка HTTPStatusError при запросе к LLM ({target_model}): {e.response.status_code} - {error_body}", exc_info=False) # exc_info=False чтобы не дублировать стектрейс от raise_for_status
    except httpx.RequestError as e:
        logger.error(f"Ошибка RequestError при запросе к LLM ({target_model}): {e}", exc_info=True)
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON ответа от LLM ({target_model}): {e}. Ответ: {response.text if 'response' in locals() else 'нет ответа'}", exc_info=True)
    except Exception as e:
        logger.error(f"Неожиданная ошибка во время вызова LLM ({target_model}): {e}", exc_info=True)
    
    return None