import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient

# Определяем путь к .env файлу, который находится на один уровень выше (в корне проекта)
# относительно текущего скрипта (app/test_telegram_connection.py)
# __file__ это путь к текущему файлу скрипта
# os.path.dirname(__file__) это директория, где лежит скрипт (т.е. C:\insight-compass\app)
# os.path.join(..., '..', '.env') поднимается на уровень выше и ищет .env
script_dir = os.path.dirname(__file__)
dotenv_path = os.path.join(script_dir, '..', '.env')

# Загружаем переменные окружения из .env файла
load_dotenv(dotenv_path=dotenv_path)

# Получаем переменные окружения
API_ID_STR = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE_NUMBER = os.getenv('TELEGRAM_PHONE_NUMBER_FOR_LOGIN') # Эту переменную вы должны были добавить в .env

# --- Проверки переменных ---
if not API_ID_STR:
    print("Ошибка: Переменная окружения TELEGRAM_API_ID не найдена или пуста в .env файле.")
    exit()
if not API_HASH:
    print("Ошибка: Переменная окружения TELEGRAM_API_HASH не найдена или пуста в .env файле.")
    exit()
if not PHONE_NUMBER:
    print("Ошибка: Переменная окружения TELEGRAM_PHONE_NUMBER_FOR_LOGIN не найдена или пуста в .env файле.")
    exit()

# Преобразуем API_ID в int, так как Telethon ожидает число
try:
    API_ID = int(API_ID_STR)
except ValueError:
    print(f"Ошибка: TELEGRAM_API_ID '{API_ID_STR}' не является корректным числом.")
    exit()
# --- Конец проверок ---

# Имя сессионного файла. Telethon создаст его для сохранения данных авторизации.
# Файл будет создан в той же директории, где запущен скрипт (т.е. в C:\insight-compass\app\)
# Важно: этот файл .session не должен попадать в Git!
SESSION_NAME = 'my_telegram_session' # Будет создан файл my_telegram_session.session

async def main_logic():
    print(f"Попытка подключения к Telegram с API_ID: {API_ID}...")
    
    # Создаем экземпляр TelegramClient
    # session: имя файла сессии (без расширения .session)
    # api_id: ваш API ID
    # api_hash: ваш API HASH
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    try:
        print("Подключение к Telegram...")
        await client.connect() # Устанавливаем соединение

        # Проверяем, авторизован ли уже пользователь
        if not await client.is_user_authorized():
            print("Пользователь не авторизован. Попытка входа...")
            # Если не авторизован, Telethon запросит номер телефона (мы его уже передали),
            # затем отправит код подтверждения на этот номер в Telegram.
            await client.send_code_request(PHONE_NUMBER)
            
            # Запрашиваем у пользователя ввод кода
            code_ok = False
            while not code_ok:
                try:
                    code = input('Пожалуйста, введите код, который вы получили в Telegram: ')
                    # Пытаемся войти с номером телефона и кодом
                    await client.sign_in(phone=PHONE_NUMBER, code=code)
                    code_ok = True # Если sign_in не вызвал исключение, код верный
                except Exception as e_signin: # Обрабатываем возможные ошибки при вводе кода
                    print(f"Ошибка при входе (возможно, неверный код): {e_signin}")
                    retry = input("Попробовать ввести код еще раз? (y/n): ").lower()
                    if retry != 'y':
                        print("Отмена входа.")
                        return # Выходим из функции main_logic, если пользователь не хочет повторять
            
            print("Вход выполнен успешно!")
        else:
            print("Пользователь уже авторизован (найдена активная сессия).")

        # Если мы здесь, значит авторизация прошла успешно
        # Получаем информацию о себе (залогиненном пользователе)
        me = await client.get_me()
        if me:
            print(f"\nВы успешно вошли как:")
            print(f"  Имя: {me.first_name}")
            if me.last_name:
                print(f"  Фамилия: {me.last_name}")
            if me.username:
                print(f"  Username: @{me.username}")
            print(f"  ID пользователя: {me.id}")
        else:
            print("Не удалось получить информацию о пользователе.")
            
        # Пример: получение последних 5 сообщений из "Me" (Saved Messages)
        # Это просто для проверки, что API запросы работают
        print("\nПолучение последних сообщений из 'Saved Messages' (если есть):")
        async for message in client.iter_messages('me', limit=5):
            print(f"  ID: {message.id}, Текст: {message.text[:50] + '...' if message.text and len(message.text) > 50 else message.text}")


    except Exception as e:
        print(f"Произошла общая ошибка: {e}")
    finally:
        print("\nОтключение от Telegram.")
        if client.is_connected():
            await client.disconnect()

# Стандартный способ запуска асинхронной функции main_logic
if __name__ == '__main__':
    # В Windows может потребоваться специальная политика для asyncio, если возникают ошибки с EventLoop
    # if os.name == 'nt': # nt означает Windows
    #    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_logic())