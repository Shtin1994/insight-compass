// frontend/src/components/CommentInsightsWidget.jsx
import React, { useState, useEffect } from 'react';
// --- НАЧАЛО ИЗМЕНЕНИЯ: Исправлен путь импорта ---
import { fetchCommentInsightsAPI } from '../services/apiService'; 
// --- КОНЕЦ ИЗМЕНЕНИЯ ---

const InsightList = ({ title, items, emoji }) => {
  if (!items || items.length === 0) {
    return (
      <div>
        <h4 className="text-md font-semibold mb-1">{emoji} {title}</h4>
        <p className="text-sm text-gray-500">Нет данных для отображения.</p>
      </div>
    );
  }

  return (
    <div>
      <h4 className="text-md font-semibold mb-1">{emoji} {title}</h4>
      <ul className="list-disc list-inside pl-1 text-sm">
        {items.map((item, index) => (
          <li key={index} className="mb-0.5">
            <span className="text-gray-700">{item.text}</span>
            <span className="text-gray-500 ml-1">({item.count})</span>
          </li>
        ))}
      </ul>
    </div>
  );
};

const CommentInsightsWidget = () => {
  const [insights, setInsights] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [daysPeriod, setDaysPeriod] = useState(30);
  const [topN, setTopN] = useState(5);

  useEffect(() => {
    const loadInsights = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await fetchCommentInsightsAPI(daysPeriod, topN);
        setInsights(data);
      } catch (err) {
        setError(err.message || 'Не удалось загрузить инсайты по комментариям');
        setInsights(null);
      } finally {
        setLoading(false);
      }
    };

    loadInsights();
  }, [daysPeriod, topN]);

  const handlePeriodChange = (newPeriod) => {
    setDaysPeriod(newPeriod);
  };

  const handleTopNChange = (newTopN) => {
    setTopN(newTopN);
  };

  return (
    <div className="bg-white p-4 shadow rounded-lg">
      <div className="flex justify-between items-center mb-3">
        <h3 className="text-lg font-semibold text-gray-700">🔍 AI-Инсайты из Комментариев</h3>
        <div className="flex space-x-2">
            <select 
                value={daysPeriod} 
                onChange={(e) => handlePeriodChange(parseInt(e.target.value))}
                className="text-xs p-1 border rounded bg-gray-50 focus:ring-blue-500 focus:border-blue-500"
            >
                <option value="7">За 7 дней</option>
                <option value="30">За 30 дней</option>
                <option value="90">За 90 дней</option>
                <option value="365">За год</option>
            </select>
            <select 
                value={topN} 
                onChange={(e) => handleTopNChange(parseInt(e.target.value))}
                className="text-xs p-1 border rounded bg-gray-50 focus:ring-blue-500 focus:border-blue-500"
            >
                <option value="3">Топ 3</option>
                <option value="5">Топ 5</option>
                <option value="10">Топ 10</option>
            </select>
        </div>
      </div>

      {loading && <p className="text-center text-gray-500">Загрузка инсайтов...</p>}
      {error && <p className="text-center text-red-500">Ошибка: {error}</p>}
      
      {!loading && !error && insights && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <InsightList title="Основные Темы" items={insights.top_topics} emoji="🏷️" />
          <InsightList title="Частые Проблемы" items={insights.top_problems} emoji="😟" />
          <InsightList title="Популярные Вопросы" items={insights.top_questions} emoji="❓" />
          <InsightList title="Предложения Пользователей" items={insights.top_suggestions} emoji="💡" />
        </div>
      )}
      {!loading && !error && !insights && (
         <p className="text-center text-gray-500">Нет данных для отображения за выбранный период.</p> 
      )}
    </div>
  );
};

export default CommentInsightsWidget;