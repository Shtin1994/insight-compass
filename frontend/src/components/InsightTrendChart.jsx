// frontend/src/components/InsightTrendChart.jsx
import React, { useState, useEffect, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Label } from 'recharts';
import { fetchCommentInsightsAPI, fetchInsightItemTrendAPI } from '../services/apiService';
import useDebounce from '../hooks/useDebounce'; 

const InsightTrendChart = () => {
  const [trendData, setTrendData] = useState([]);
  const [loading, setLoading] = useState(false); 
  const [error, setError] = useState(null);

  const [itemType, setItemType] = useState('topic'); 
  const [itemText, setItemText] = useState('');     
  const [daysPeriod, setDaysPeriod] = useState(30);
  const [granularity, setGranularity] = useState('day'); 
  
  const [availableTopics, setAvailableTopics] = useState([]);
  const [availableProblems, setAvailableProblems] = useState([]);
  // Добавим состояния для других типов, если решим делать для них списки
  // const [availableQuestions, setAvailableQuestions] = useState([]);
  // const [availableSuggestions, setAvailableSuggestions] = useState([]);


  const [initialLoadDone, setInitialLoadDone] = useState(false);
  const debouncedItemText = useDebounce(itemText, 700); // Увеличим немного задержку для текстового ввода

  // Загрузка доступных тем/проблем для селекторов и дефолтного выбора
  useEffect(() => {
    const fetchInitialSelectOptions = async () => {
      // Не ставим setLoading(true) здесь, чтобы не было двойного лоадера с основным графиком
      try {
        const insights = await fetchCommentInsightsAPI(daysPeriod, 15); // Берем топ-15 для разнообразия в селекторах
        let defaultTextForTrend = '';
        let defaultTypeForTrend = itemType; // Сохраняем текущий тип, если он уже выбран

        if (insights) {
          if (insights.top_topics && insights.top_topics.length > 0) {
            setAvailableTopics(insights.top_topics.map(t => t.text));
            if (!itemText && defaultTypeForTrend === 'topic') { // Устанавливаем дефолтный текст только если он еще не задан
              defaultTextForTrend = insights.top_topics[0].text;
            }
          } else {
            setAvailableTopics([]);
          }

          if (insights.top_problems && insights.top_problems.length > 0) {
            setAvailableProblems(insights.top_problems.map(p => p.text));
            if (!itemText && defaultTypeForTrend === 'problem' && defaultTextForTrend === '') {
              defaultTextForTrend = insights.top_problems[0].text;
            }
          } else {
            setAvailableProblems([]);
          }
          // Можно добавить загрузку для questions и suggestions аналогично, если нужно
        }
        
        // Устанавливаем текст, если он был определен как дефолтный и еще не задан
        if (defaultTextForTrend && !itemText) {
            setItemText(defaultTextForTrend);
        }

      } catch (err) {
        console.error("Ошибка при загрузке опций для селекторов тренда:", err);
        setAvailableTopics([]);
        setAvailableProblems([]);
      } finally {
        setInitialLoadDone(true); 
      }
    };
    fetchInitialSelectOptions();
  // eslint-disable-next-line react-hooks/exhaustive-deps 
  }, [daysPeriod]); // Перезагружаем опции, если изменился основной период (чтобы подсказки были релевантны)
                 // itemText и itemType убраны из зависимостей, чтобы не было зацикливания при установке дефолтного itemText


  const loadTrendData = useCallback(async () => {
    // Используем debouncedItemText для текстового поля, или itemText если это выбор из <select>
    const textToFetch = (itemType === 'topic' || itemType === 'problem') ? itemText : debouncedItemText;

    if (!textToFetch || !itemType) {
      setTrendData([]);
      return;
    }
    
    setLoading(true);
    setError(null);
    console.log(`Запрос тренда для: тип=${itemType}, текст="${textToFetch}", период=${daysPeriod}, гранулярность=${granularity}`);
    try {
      const data = await fetchInsightItemTrendAPI(itemType, textToFetch, daysPeriod, granularity);
      setTrendData(data.trend_data || []);
    } catch (err) {
      setError(err.message || 'Не удалось загрузить данные для графика тренда');
      setTrendData([]);
    } finally {
      setLoading(false);
    }
  }, [itemType, itemText, debouncedItemText, daysPeriod, granularity]); // Добавили itemText

  useEffect(() => {
     if (initialLoadDone) { 
        // Для типов topic/problem, где есть select, грузим сразу при изменении itemText
        // Для question/suggestion, где текстовый ввод, грузим по debouncedItemText
        if ((itemType === 'topic' || itemType === 'problem') && itemText) {
            loadTrendData();
        } else if ((itemType === 'question' || itemType === 'suggestion') && debouncedItemText) {
            loadTrendData();
        } else if (!itemText && !debouncedItemText) { // Если текст очищен
            setTrendData([]);
        }
     }
  }, [initialLoadDone, itemType, itemText, debouncedItemText, loadTrendData]);


  const handleItemTypeChange = (e) => {
    const newType = e.target.value;
    setItemType(newType);
    // Устанавливаем дефолтный текст, если он есть для нового типа, или сбрасываем
    if (newType === 'topic' && availableTopics.length > 0) setItemText(availableTopics[0]);
    else if (newType === 'problem' && availableProblems.length > 0) setItemText(availableProblems[0]);
    else setItemText(''); 
  };
  
  const handleItemTextChange = (e) => {
    setItemText(e.target.value);
  };
  
  const handleDaysPeriodChange = (e) => setDaysPeriod(parseInt(e.target.value));
  const handleGranularityChange = (e) => setGranularity(e.target.value);

  const renderItemTextControl = () => {
    if (itemType === 'topic' && availableTopics.length > 0) {
      return (
        <select 
          id="itemTextSelectTopic" 
          value={itemText} 
          onChange={handleItemTextChange}
          className="w-full text-sm p-2 border border-gray-300 rounded-md bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
        >
          <option value="">-- Выберите тему --</option>
          {availableTopics.map(topic => <option key={topic} value={topic}>{topic}</option>)}
        </select>
      );
    }
    if (itemType === 'problem' && availableProblems.length > 0) {
      return (
        <select 
          id="itemTextSelectProblem" 
          value={itemText} 
          onChange={handleItemTextChange}
          className="w-full text-sm p-2 border border-gray-300 rounded-md bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
        >
          <option value="">-- Выберите проблему --</option>
          {availableProblems.map(problem => <option key={problem} value={problem}>{problem}</option>)}
        </select>
      );
    }
    // Для questions и suggestions, или если списки тем/проблем пусты, используем текстовое поле
    return (
      <input 
        type="text"
        id="itemTextInput"
        value={itemText}
        onChange={handleItemTextChange}
        placeholder="Введите текст инсайта..."
        className="w-full text-sm p-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
      />
    );
  };

  return (
    <div className="bg-white p-6 shadow-lg rounded-xl">
      <h3 className="text-xl font-semibold text-gray-800 mb-1">📊 Тренд Инсайта</h3>
      
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3 items-end mb-4 p-3 bg-gray-50 rounded-md">
        <div>
          <label htmlFor="itemType" className="block text-xs font-medium text-gray-600 mb-1">Тип</label>
          <select id="itemType" value={itemType} onChange={handleItemTypeChange} className="w-full text-sm p-2 border border-gray-300 rounded-md bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500">
            <option value="topic">Тема</option>
            <option value="problem">Проблема</option>
            <option value="question">Вопрос</option>
            <option value="suggestion">Предложение</option>
          </select>
        </div>
        
        <div className="lg:col-span-1"> {/* Убрал col-span-2, чтобы все элементы были примерно одинаковой ширины */}
          <label htmlFor="itemTextControl" className="block text-xs font-medium text-gray-600 mb-1">Текст инсайта</label>
          {renderItemTextControl()}
        </div>

        <div>
          <label htmlFor="daysPeriod" className="block text-xs font-medium text-gray-600 mb-1">Период</label>
          <select id="daysPeriod" value={daysPeriod} onChange={handleDaysPeriodChange} className="w-full text-sm p-2 border border-gray-300 rounded-md bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500">
            <option value="7">7 дней</option>
            <option value="30">30 дней</option>
            <option value="90">90 дней</option>
            <option value="365">Год</option>
          </select>
        </div>
        
        <div>
          <label htmlFor="granularity" className="block text-xs font-medium text-gray-600 mb-1">Гранулярность</label>
          <select id="granularity" value={granularity} onChange={handleGranularityChange} className="w-full text-sm p-2 border border-gray-300 rounded-md bg-white focus:ring-1 focus:ring-blue-500 focus:border-blue-500">
            <option value="day">По дням</option>
            <option value="week">По неделям</option>
          </select>
        </div>
      </div>
      
      {loading && <div className="text-center py-10 text-gray-500">Загрузка графика...</div>}
      {error && <div className="text-center py-10 text-red-600 bg-red-50 p-3 rounded-md">Ошибка: {error}</div>}
      
      {!loading && !error && itemText && trendData.length > 0 && (
        <div style={{ width: '100%', height: 300 }}>
          <ResponsiveContainer>
            <LineChart data={trendData} margin={{ top: 5, right: 30, left: 0, bottom: 25 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
              <XAxis dataKey="date" angle={-35} textAnchor="end" height={60} tick={{ fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis allowDecimals={false} tick={{ fontSize: 10 }}>
                  <Label value="Упоминания" angle={-90} position="insideLeft" style={{ textAnchor: 'middle', fontSize: 12, fill: '#666' }} />
              </YAxis>
              <Tooltip contentStyle={{ fontSize: 12, padding: '5px 10px', borderRadius: '6px' }} />
              <Legend verticalAlign="top" height={36} iconSize={10} wrapperStyle={{fontSize: "12px"}} />
              <Line type="monotone" dataKey="count" name={`"${itemText}"`} stroke="#10b981" strokeWidth={2} activeDot={{ r: 7 }} dot={{r:4, strokeWidth: 2, fill: '#fff'}} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
      
      {!loading && !error && itemText && trendData.length === 0 && (
        <div className="min-h-[250px] flex justify-center items-center">
          <p className="text-gray-500">Нет данных о тренде для "{itemText}" за выбранный период.</p>
        </div>
      )}
       {!loading && !error && !itemText && (
        <div className="min-h-[250px] flex justify-center items-center">
          <p className="text-gray-500">Выберите или введите текст инсайта для отображения тренда.</p>
        </div>
      )}
    </div>
  );
};

export default InsightTrendChart;