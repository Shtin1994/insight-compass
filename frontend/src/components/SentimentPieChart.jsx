// frontend/src/components/SentimentPieChart.jsx
import React, { useState, useEffect } from 'react';
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { fetchSentimentDistributionAPI } from '../services/apiService'; // Эту функцию нужно будет добавить

// Определим цвета для разных тональностей
const SENTIMENT_COLORS = {
  positive: '#82ca9d', // Зеленый
  negative: '#ff7300', // Оранжево-красный
  neutral: '#8884d8',  // Фиолетовый
  mixed: '#ffc658',    // Желтый
  undefined: '#cccccc' // Серый для неопределенных
};

const RADIAN = Math.PI / 180;
const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, index, payload }) => {
  if (percent < 0.03) return null; // Не показываем метку, если сектор слишком мал
  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);

  return (
    <text x={x} y={y} fill="white" textAnchor={x > cx ? 'start' : 'end'} dominantBaseline="central">
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};


function SentimentPieChart() {
  const [sentimentData, setSentimentData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [daysPeriod, setDaysPeriod] = useState(7); // По умолчанию 7 дней

  useEffect(() => {
    const loadSentimentData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const response = await fetchSentimentDistributionAPI(daysPeriod);
        // Recharts PieChart ожидает массив объектов с ключами name и value
        // Исключим 'undefined', если его count равен 0, для более чистой диаграммы,
        // или можно всегда показывать, если это важно.
        const chartData = response.data
          .filter(item => item.sentiment_label !== 'undefined' || item.count > 0) // Показываем 'undefined' только если есть count > 0
          .map(item => ({
            name: item.sentiment_label.charAt(0).toUpperCase() + item.sentiment_label.slice(1), // "Positive", "Negative", etc.
            value: item.count,
          }));
        setSentimentData({ ...response, chartData }); // Сохраняем и исходные данные, и данные для графика
      } catch (err) {
        setError(err.message);
        setSentimentData(null);
      } finally {
        setIsLoading(false);
      }
    };
    loadSentimentData();
  }, [daysPeriod]);

  if (isLoading) {
    return (
      <div className="spinner-container" style={{ minHeight: '300px' }}>
        <div className="spinner"></div>
      </div>
    );
  }

  if (error) {
    return <p>Ошибка загрузки распределения тональности: {error}</p>;
  }

  if (!sentimentData || !sentimentData.chartData || sentimentData.chartData.length === 0 || sentimentData.total_analyzed_posts === 0) {
    return (
        <div className="sentiment-pie-chart" style={{ marginBottom: '30px' }}>
            <h3>Распределение тональности постов за {daysPeriod} дней</h3>
            <p>Нет данных о тональности для отображения за выбранный период.</p>
            <div style={{ marginBottom: '10px' }}>
                <label htmlFor="sentiment-days-select">Период: </label>
                <select id="sentiment-days-select" value={daysPeriod} onChange={(e) => setDaysPeriod(Number(e.target.value))}>
                <option value="7">7 дней</option>
                <option value="30">30 дней</option>
                <option value="90">90 дней</option>
                </select>
            </div>
        </div>
    );
  }
  
  return (
    <div className="sentiment-pie-chart" style={{ marginBottom: '30px' }}>
      <h3>Распределение тональности постов за {daysPeriod} дней (всего проанализировано: {sentimentData.total_analyzed_posts})</h3>
      <div style={{ marginBottom: '10px' }}>
        <label htmlFor="sentiment-days-select">Период: </label>
        <select id="sentiment-days-select" value={daysPeriod} onChange={(e) => setDaysPeriod(Number(e.target.value))}>
          <option value="7">7 дней</option>
          <option value="30">30 дней</option>
          <option value="90">90 дней</option>
        </select>
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <PieChart>
          <Pie
            data={sentimentData.chartData}
            cx="50%"
            cy="50%"
            labelLine={false}
            label={renderCustomizedLabel}
            outerRadius={100}
            fill="#8884d8"
            dataKey="value"
            nameKey="name"
          >
            {sentimentData.chartData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={SENTIMENT_COLORS[entry.name.toLowerCase()] || SENTIMENT_COLORS.undefined} />
            ))}
          </Pie>
          <Tooltip />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

export default SentimentPieChart;