// frontend/src/components/CommentInsightsWidget.jsx
import React, { useState, useEffect } from 'react';
// Убедитесь, что этот путь правильный для вашей структуры:
// Если CommentInsightsWidget.jsx в src/components/
// и apiService.js в src/services/
// то путь ДОЛЖЕН БЫТЬ '../services/apiService'
import { fetchCommentInsightsAPI } from '../services/apiService'; 

// Вспомогательная функция для определения размера шрифта
const calculateFontSize = (count, minCount, maxCount, minFontSize = 0.8, maxFontSize = 1.5) => {
  if (maxCount === minCount || count <= minCount) {
    return `${minFontSize}rem`;
  }
  const percentage = (count - minCount) / (maxCount - minCount);
  const fontSize = minFontSize + percentage * (maxFontSize - minFontSize);
  return `${fontSize.toFixed(2)}rem`;
};

const InsightList = ({ title, items, emoji, isTagCloud = false }) => {
  if (!items || items.length === 0) {
    return (
      <div>
        <h4 className="text-md font-semibold mb-2">{emoji} {title}</h4>
        <p className="text-sm text-gray-500">Нет данных для отображения.</p>
      </div>
    );
  }

  let minCount = Infinity;
  let maxCount = -Infinity;
  if (isTagCloud && items.length > 0) {
    items.forEach(item => {
      if (item.count < minCount) minCount = item.count;
      if (item.count > maxCount) maxCount = item.count;
    });
  }
  if (isTagCloud && items.length === 1) {
    minCount = maxCount = items[0].count;
  }

  return (
    <div>
      <h4 className="text-md font-semibold mb-2">{emoji} {title}</h4>
      {isTagCloud ? (
        <div className="flex flex-wrap gap-x-3 gap-y-1 items-baseline">
          {items.map((item, index) => (
            <span 
              key={index} 
              className="text-gray-700 hover:text-blue-600 cursor-default transition-colors duration-150"
              style={{ fontSize: calculateFontSize(item.count, minCount, maxCount) }}
              title={`${item.text} (${item.count})`}
            >
              {item.text}
              <span className="text-gray-400" style={{ fontSize: '0.7rem' }}> ({item.count})</span>
            </span>
          ))}
        </div>
      ) : (
        <ul className="list-disc list-inside pl-1 text-sm space-y-1">
          {items.map((item, index) => (
            <li key={index}>
              <span className="text-gray-700">{item.text}</span>
              <span className="text-gray-500 ml-1">({item.count})</span>
            </li>
          ))}
        </ul>
      )}
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
    <div className="bg-white p-6 shadow-lg rounded-xl">
      <div className="flex flex-col sm:flex-row justify-between sm:items-center mb-4 gap-3 sm:gap-0">
        <h3 className="text-xl font-semibold text-gray-800">🔍 AI-Инсайты из Комментариев</h3>
        <div className="flex space-x-3">
            <select 
                value={daysPeriod} 
                onChange={(e) => handlePeriodChange(parseInt(e.target.value))}
                className="text-sm p-2 border border-gray-300 rounded-md bg-white hover:border-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
            >
                <option value="7">За 7 дней</option>
                <option value="30">За 30 дней</option>
                <option value="90">За 90 дней</option>
                <option value="365">За год</option>
            </select>
            <select 
                value={topN} 
                onChange={(e) => handleTopNChange(parseInt(e.target.value))}
                className="text-sm p-2 border border-gray-300 rounded-md bg-white hover:border-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
            >
                <option value="3">Топ 3</option>
                <option value="5">Топ 5</option>
                <option value="10">Топ 10</option>
                <option value="15">Топ 15</option>
            </select>
        </div>
      </div>

      {loading && <div className="text-center py-10 text-gray-500">Загрузка инсайтов...</div>}
      {error && <div className="text-center py-10 text-red-600 bg-red-50 p-3 rounded-md">Ошибка: {error}</div>}
      
      {!loading && !error && insights && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-8">
          <InsightList title="Основные Темы" items={insights.top_topics} emoji="🏷️" isTagCloud={true} />
          <InsightList title="Частые Проблемы" items={insights.top_problems} emoji="😟" isTagCloud={true} />
          <InsightList title="Популярные Вопросы" items={insights.top_questions} emoji="❓" />
          <InsightList title="Предложения Пользователей" items={insights.top_suggestions} emoji="💡" />
        </div>
      )}
      {!loading && !error && !insights && (
         <div className="text-center py-10 text-gray-500">Нет данных для отображения за выбранный период. Попробуйте выбрать другой период или убедитесь, что AI-анализ комментариев был выполнен.</div> 
      )}
    </div>
  );
};

export default CommentInsightsWidget;