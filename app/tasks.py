import asyncio
import os # Перемещен импорт os наверх, так как он используется в _async_main_collection_logic
import time # Перемещен импорт time наверх
from app.celery_app import celery_instance # Наш экземпляр Celery
from app.core.config import settings # Наши настройки, включая ключи и список каналов
from telethon import TelegramClient


# --- Существующие тестовые задачи ---
@celery_instance.task(name="add_numbers")
def add(x: int, y: int) -> int:
    print(f"Celery таск 'add_numbers': Получены числа {x} и {y}")
    # import time # Уже импортирован выше
    time.sleep(5) 
    result = x + y
    print(f"Celery таск 'add_numbers': Результат сложения {x} + {y} = {result}")
    return result

@celery_instance.task(name="simple_debug_task")
def simple_debug_task(message: str):
    print(f"Сообщение от simple_debug_task: {message}")
    return f"Сообщение '{message}' обработано Celery!"
# --- Конец существующих тестовых задач ---


# НОВЫЙ ТАСК для сбора данных (ИСПРАВЛЕННАЯ ВЕРСИЯ)
@celery_instance.task(name="collect_telegram_data")
def collect_telegram_data_task():
    """
    Основной таск для сбора данных из Telegram-каналов.
    """
    print("Запущен таск collect_telegram_data_task (версия с явным loop и исправленными переменными)...")

    # Получаем учетные данные и список каналов из настроек
    # Эти переменные должны быть доступны во внутренней функции _async_main_collection_logic
    # Поэтому мы либо передаем их как аргументы, либо определяем их там, где они используются.
    # Либо делаем их nonlocal, если _async_main_collection_logic остается вложенной.
    # Проще всего - определить их внутри _async_main_collection_logic или передать.
    # Сейчас они будут доступны через замыкание, так как _async_main_collection_logic вложена.

    api_id_val = settings.TELEGRAM_API_ID
    api_hash_val = settings.TELEGRAM_API_HASH
    phone_number_val = settings.TELEGRAM_PHONE_NUMBER_FOR_LOGIN
    target_channels_list = settings.TARGET_TELEGRAM_CHANNELS

    if not all([api_id_val, api_hash_val, phone_number_val]):
        print("Ошибка: Необходимые учетные данные Telegram (API_ID, API_HASH, PHONE_NUMBER) не настроены.")
        return "Ошибка конфигурации Telegram"

    session_file_path_in_container = "/app/my_telegram_session" # Telethon сам добавит .session

    async def _async_main_collection_logic(event_loop):
        # Используем значения, полученные из settings во внешней функции
        client = TelegramClient(session_file_path_in_container, api_id_val, api_hash_val, loop=event_loop)
        try:
            print(f"Подключение к Telegram из Celery таска, сессия: {session_file_path_in_container}.session")
            await client.connect()

            if not await client.is_user_authorized():
                print(f"ОШИБКА: Пользователь не авторизован в Celery таске! Используется сессия: {session_file_path_in_container}.session")
                expected_session_file = f"{session_file_path_in_container}.session"
                if os.path.exists(expected_session_file):
                    print(f"Файл сессии {expected_session_file} СУЩЕСТВУЕТ.")
                    stat_info = os.stat(expected_session_file)
                    print(f"  Права доступа: {oct(stat_info.st_mode)[-3:]}, Размер: {stat_info.st_size} байт")
                else:
                    print(f"Файл сессии {expected_session_file} НЕ НАЙДЕН.")
                return "Ошибка авторизации Telegram в Celery"
            
            me = await client.get_me()
            print(f"Celery таск успешно подключен как: {me.first_name} (@{me.username or ''})")

            for channel_identifier in target_channels_list:
                print(f"\nОбработка канала: {channel_identifier}")
                try:
                    channel_entity = await client.get_entity(channel_identifier)
                    print(f"  ID канала: {channel_entity.id}")
                    print(f"  Название: {channel_entity.title}")
                    # TODO: Логика сбора постов и комментариев
                except ValueError as ve:
                    print(f"  Ошибка при получении информации о канале {channel_identifier}: {ve}")
                except Exception as e_channel:
                    print(f"  Неожиданная ошибка при обработке канала {channel_identifier}: {e_channel}")
            
            return "Сбор данных завершен (базовая версия)"

        except Exception as e_main:
            print(f"Главная ошибка в асинхронной логике Celery таска: {e_main}")
            import traceback
            traceback.print_exc()
            return f"Ошибка в Celery таске: {str(e_main)}"
        finally:
            if client and client.is_connected():
                print("Отключение от Telegram в Celery таске.")
                await client.disconnect()

    # Блок try...except...finally для управления event loop'ом
    result_message = "Не удалось инициализировать event loop" # Начальное значение
    loop = None 
    
    try:
        # os уже импортирован наверху файла
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result_message = loop.run_until_complete(_async_main_collection_logic(loop))
    except Exception as e_run:
        print(f"Ошибка при управлении event loop'ом в Celery таске: {e_run}")
        import traceback
        traceback.print_exc()
        result_message = f"Критическая ошибка выполнения asyncio в Celery: {str(e_run)}"
    finally:
        if loop and not loop.is_closed():
            loop.close()
        
    print(f"Результат выполнения collect_telegram_data_task: {result_message}")
    return result_message

# В будущем здесь будет настройка Celery Beat для периодического запуска этого таска