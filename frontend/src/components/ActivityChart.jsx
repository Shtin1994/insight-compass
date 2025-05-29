// frontend/src/components/ActivityChart.jsx
import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { fetchActivityOverTimeAPI } from '../services/apiService'; // Мы создадим эту функцию

function ActivityChart() {
  const [activityData, setActivityData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [days, setDays] = useState(7); // По умолчанию показываем за 7 дней

  useEffect(() => {
    const loadActivityData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const data = await fetchActivityOverTimeAPI(days);
        // Recharts ожидает данные в определенном формате. Убедимся, что даты отформатированы.
        const formattedData = data.data.map(item => ({
          ...item,
          // Форматируем дату для оси X, если нужно (например, ДД.ММ)
          // activity_date уже в формате YYYY-MM-DD, что Recharts должен понять
          // Если хотим кастомный формат:
          // display_date: new Date(item.activity_date).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' })
        }));
        setActivityData(formattedData);
      } catch (err) {
        setError(err.message);
        setActivityData([]);
      } finally {
        setIsLoading(false);
      }
    };
    loadActivityData();
  }, [days]); // Перезагружаем при изменении 'days'

  if (isLoading) {
    return (
      <div className="spinner-container" style={{ height: '300px' }}> {/* Задаем высоту для спиннера графика */}
        <div className="spinner"></div>
      </div>
    );
  }

  if (error) {
    return <p>Ошибка загрузки данных для графика активности: {error}</p>;
  }

  if (!activityData.length) {
    return <p>Нет данных для отображения графика активности.</p>;
  }

  return (
    <div className="activity-chart" style={{ marginBottom: '30px' }}>
      <h3>Активность за последние {days} дней</h3>
      {/* Кнопки для смены периода (опционально, можно добавить позже) */}
      {/* <div>
        <button onClick={() => setDays(7)} disabled={days === 7}>7 дней</button>
        <button onClick={() => setDays(30)} disabled={days === 30}>30 дней</button>
      </div> */}
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={activityData} margin={{ top: 5, right: 20, left: -20, bottom: 5 }}> {/* Уменьшил левый отступ */}
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="activity_date" 
                 tickFormatter={(tick) => new Date(tick).toLocaleDateString('ru-RU', {day: 'numeric', month: 'short'})} 
          />
          <YAxis yAxisId="left" allowDecimals={false} />
          <YAxis yAxisId="right" orientation="right" allowDecimals={false} />
          <Tooltip 
            labelFormatter={(label) => new Date(label).toLocaleDateString('ru-RU', {weekday: 'short', day: 'numeric', month: 'short'})}
          />
          <Legend />
          <Line yAxisId="left" type="monotone" dataKey="post_count" stroke="#8884d8" name="Посты" activeDot={{ r: 8 }} />
          <Line yAxisId="right" type="monotone" dataKey="comment_count" stroke="#82ca9d" name="Комментарии" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export default ActivityChart;