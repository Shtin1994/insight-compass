// frontend/src/components/SentimentPieChart.jsx
import React, { useState, useEffect } from 'react';
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { fetchSentimentDistributionAPI } from '../services/apiService';

// Цвета для разных тональностей
const SENTIMENT_COLORS = {
  positive: '#28a745', // Зеленый (Bootstrap success)
  negative: '#dc3545', // Красный (Bootstrap danger)
  neutral: '#6c757d',  // Серый (Bootstrap secondary)
  mixed: '#ffc107',    // Желтый (Bootstrap warning)
  undefined: '#e9ecef' // Светло-серый (Bootstrap light)
};

const RADIAN = Math.PI / 180;
const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, index, payload }) => {
  if (percent < 0.03 && payload.value > 0) { // Показываем, если значение не 0, даже если процент мал
    const radius = outerRadius + 15; // Выносим маленькие метки подальше
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);
    return (
      <text x={x} y={y} fill={SENTIMENT_COLORS[payload.name.toLowerCase()] || SENTIMENT_COLORS.undefined} textAnchor={x > cx ? 'start' : 'end'} dominantBaseline="central" fontSize="12px">
        {`${payload.name} ${(percent * 100).toFixed(0)}%`}
      </text>
    );
  }
  if (percent === 0) return null; // Не показываем метку для нулевых значений

  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);

  return (
    <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central" fontWeight="bold">
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};


function SentimentPieChart() {
  const [sentimentData, setSentimentData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [daysPeriod, setDaysPeriod] = useState(7);

  useEffect(() => {
    const loadSentimentData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const response = await fetchSentimentDistributionAPI(daysPeriod);
        const chartData = response.data
          .filter(item => item.count > 0) // Отображаем только те категории, где есть посты
          .map(item => ({
            name: item.sentiment_label.charAt(0).toUpperCase() + item.sentiment_label.slice(1),
            value: item.count,
          }));
        setSentimentData({ ...response, chartData });
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
      <div className="spinner-container" style={{ minHeight: '350px' }}> {/* Увеличим высоту для диаграммы */}
        <div className="spinner"></div>
      </div>
    );
  }

  if (error) {
    return <p>Ошибка загрузки распределения тональности: {error}</p>;
  }

  if (!sentimentData || !sentimentData.chartData || sentimentData.chartData.length === 0 || sentimentData.total_analyzed_posts === 0) {
    return (
        <div className="sentiment-pie-chart" style={{ marginTop: '30px' }}>
            <h3>Распределение тональности постов за {daysPeriod} дней</h3>
            <div style={{ marginBottom: '10px' }}>
                <label htmlFor="sentiment-days-select">Период: </label>
                <select id="sentiment-days-select" value={daysPeriod} onChange={(e) => setDaysPeriod(Number(e.target.value))}>
                <option value="7">7 дней</option>
                <option value="30">30 дней</option>
                <option value="90">90 дней</option>
                </select>
            </div>
            <p>Нет данных о тональности для отображения за выбранный период.</p>
        </div>
    );
  }
  
  return (
    <div className="sentiment-pie-chart" style={{ marginTop: '30px' }}>
      <h3>Распределение тональности постов за {daysPeriod} дней (всего проанализировано: {sentimentData.total_analyzed_posts})</h3>
      <div style={{ marginBottom: '10px' }}>
        <label htmlFor="sentiment-days-select">Период: </label>
        <select id="sentiment-days-select" value={daysPeriod} onChange={(e) => setDaysPeriod(Number(e.target.value))}>
          <option value="7">7 дней</option>
          <option value="30">30 дней</option>
          <option value="90">90 дней</option>
        </select>
      </div>
      <ResponsiveContainer width="100%" height={350}> {/* Увеличим высоту */}
        <PieChart>
          <Pie
            data={sentimentData.chartData}
            cx="50%"
            cy="50%"
            labelLine={false}
            label={renderCustomizedLabel}
            outerRadius={110} // Немного увеличим радиус
            fill="#8884d8"
            dataKey="value"
            nameKey="name"
          >
            {sentimentData.chartData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={SENTIMENT_COLORS[entry.name.toLowerCase()] || SENTIMENT_COLORS.undefined} />
            ))}
          </Pie>
          <Tooltip formatter={(value, name, props) => [`${value} (${(props.payload.percent * 100).toFixed(1)}%)`, name]}/>
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

export default SentimentPieChart;