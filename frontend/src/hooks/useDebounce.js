// frontend/src/hooks/useDebounce.js

import { useState, useEffect } from 'react';

/**
 * Пользовательский хук для debounce значения.
 * Возвращает значение только после указанной задержки с момента последнего изменения.
 * Полезно для отложенной отправки запросов при вводе текста.
 *
 * @param {*} value - Значение, которое нужно "задебаунсить".
 * @param {number} delay - Задержка в миллисекундах.
 * @returns {*} - "Задебаунсенное" значение.
 */
function useDebounce(value, delay) {
  // Состояние для хранения задебаунсенного значения
  const [debouncedValue, setDebouncedValue] = useState(value);

  useEffect(() => {
    // Устанавливаем таймер для обновления задебаунсенного значения
    const handler = setTimeout(() => {
      setDebouncedValue(value);
    }, delay);

    // Очищаем предыдущий таймер при каждом изменении значения (value) или задержки (delay)
    // Это происходит до выполнения нового эффекта или при размонтировании компонента
    return () => {
      clearTimeout(handler);
    };
  }, [value, delay]); // Зависимости: переустанавливаем таймер при каждом изменении value или delay

  return debouncedValue;
}

export default useDebounce;