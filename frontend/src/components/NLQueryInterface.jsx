// frontend/src/components/NLQueryInterface.jsx
import React, { useState } from 'react';
import { postNLQueryAPI } from '../services/apiService';

const NLQueryInterface = () => {
  const [queryText, setQueryText] = useState('');
  const [aiAnswer, setAiAnswer] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  // --- НАЧАЛО ИЗМЕНЕНИЙ: Состояние для выбранного периода ---
  const [selectedDaysPeriod, setSelectedDaysPeriod] = useState(''); // Пустая строка - "не указан" / из текста
  // --- КОНЕЦ ИЗМЕНЕНИЙ ---

  const handleSubmitQuery = async (e) => {
    e.preventDefault();
    if (!queryText.trim()) {
      setError('Пожалуйста, введите ваш вопрос.');
      return;
    }

    setIsLoading(true);
    setError(null);
    setAiAnswer('');

    // --- НАЧАЛО ИЗМЕНЕНИЙ: Формирование contextParams ---
    const contextParams = {};
    if (selectedDaysPeriod && selectedDaysPeriod !== '') {
      contextParams.days_period = parseInt(selectedDaysPeriod, 10);
    }
    // --- КОНЕЦ ИЗМЕНЕНИЙ ---

    try {
      // Передаем contextParams в API
      const response = await postNLQueryAPI(queryText, contextParams); 
      if (response && response.ai_answer) {
        setAiAnswer(response.ai_answer);
      } else {
        setAiAnswer(response.error_message || 'Не удалось получить четкий ответ от AI.'); // Отображаем error_message, если есть
      }
    } catch (err) {
      setError(err.message || 'Произошла ошибка при отправке запроса.');
      setAiAnswer('');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="bg-white p-6 shadow-lg rounded-xl mt-8">
      <h3 className="text-xl font-semibold text-gray-800 mb-4">💬 Спросите Инсайт-Компас</h3>
      
      <form onSubmit={handleSubmitQuery} className="space-y-4">
        <div>
          <label htmlFor="nlQueryInput" className="block text-sm font-medium text-gray-700 mb-1">
            Ваш вопрос:
          </label>
          <textarea
            id="nlQueryInput"
            rows="3"
            className="w-full text-sm p-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 shadow-sm"
            value={queryText}
            onChange={(e) => setQueryText(e.target.value)}
            placeholder="Например: Какие основные темы обсуждались за последние 7 дней?"
            disabled={isLoading}
          />
        </div>

        {/* --- НАЧАЛО ИЗМЕНЕНИЙ: Селектор для периода --- */}
        <div>
          <label htmlFor="nlQueryDaysPeriod" className="block text-sm font-medium text-gray-700 mb-1">
            Период для вопроса (опционально):
          </label>
          <select
            id="nlQueryDaysPeriod"
            value={selectedDaysPeriod}
            onChange={(e) => setSelectedDaysPeriod(e.target.value)}
            className="w-full sm:w-1/2 lg:w-1/3 text-sm p-2 border border-gray-300 rounded-md bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
            disabled={isLoading}
          >
            <option value="">Из текста вопроса / По умолчанию</option>
            <option value="1">За 1 день</option>
            <option value="7">За 7 дней</option>
            <option value="30">За 30 дней</option>
            <option value="90">За 90 дней</option>
            <option value="365">За год</option>
          </select>
          <p className="text-xs text-gray-500 mt-1">Если не выбран, система попытается определить период из текста вопроса или использует значение по умолчанию (7 дней).</p>
        </div>
        {/* --- КОНЕЦ ИЗМЕНЕНИЙ --- */}
        
        <div>
          <button
            type="submit"
            className="w-full sm:w-auto inline-flex justify-center items-center px-6 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
            disabled={isLoading || !queryText.trim()}
          >
            {isLoading ? (
              <>
                <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                Обработка...
              </>
            ) : (
              'Спросить AI'
            )}
          </button>
        </div>
      </form>

      {error && (
        <div className="mt-4 p-3 bg-red-50 text-red-700 border border-red-200 rounded-md text-sm">
          <strong>Ошибка:</strong> {error}
        </div>
      )}

      {aiAnswer && !isLoading && !error && (
        <div className="mt-6 p-4 bg-gray-50 border border-gray-200 rounded-md">
          <h4 className="text-md font-semibold text-gray-700 mb-2">Ответ AI:</h4>
          <p className="text-sm text-gray-800 whitespace-pre-wrap">{aiAnswer}</p>
        </div>
      )}
    </div>
  );
};

export default NLQueryInterface;