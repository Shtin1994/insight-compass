// frontend/src/components/TopChannels.jsx
import React, { useState, useEffect } from 'react';
import { fetchTopChannelsAPI } from '../services/apiService'; // Эту функцию нужно будет добавить

function TopChannels() {
  const [topChannelsData, setTopChannelsData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const [metric, setMetric] = useState('posts'); // 'posts' или 'comments'
  const [limit, setLimit] = useState(5);
  const [daysPeriod, setDaysPeriod] = useState(7);

  useEffect(() => {
    const loadTopChannels = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const data = await fetchTopChannelsAPI(metric, limit, daysPeriod);
        setTopChannelsData(data);
      } catch (err) {
        setError(err.message);
        setTopChannelsData(null);
      } finally {
        setIsLoading(false);
      }
    };
    loadTopChannels();
  }, [metric, limit, daysPeriod]); // Перезагружаем при смене параметров

  if (isLoading) {
    return (
      <div className="spinner-container" style={{ minHeight: '150px' }}>
        <div className="spinner"></div>
      </div>
    );
  }

  if (error) {
    return <p>Ошибка загрузки топ каналов: {error}</p>;
  }

  if (!topChannelsData || !topChannelsData.data || topChannelsData.data.length === 0) {
    return <p>Нет данных для отображения топ каналов по выбранным критериям.</p>;
  }

  const metricNameDisplay = topChannelsData.metric_name === 'posts' ? 'Постов' : 'Комментариев';

  return (
    <div className="top-channels" style={{ marginBottom: '30px' }}>
      <h3>Топ-{limit} каналов по количеству {metric === 'posts' ? 'постов' : 'комментариев'} за {daysPeriod} дней</h3>
      
      <div style={{ marginBottom: '10px' }}>
        <label htmlFor="metric-select">Метрика: </label>
        <select id="metric-select" value={metric} onChange={(e) => setMetric(e.target.value)}>
          <option value="posts">Посты</option>
          <option value="comments">Комментарии</option>
        </select>
        
        <label htmlFor="days-select" style={{ marginLeft: '15px' }}>Период: </label>
        <select id="days-select" value={daysPeriod} onChange={(e) => setDaysPeriod(Number(e.target.value))}>
          <option value="7">7 дней</option>
          <option value="30">30 дней</option>
          <option value="90">90 дней</option>
        </select>
      </div>

      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Канал</th>
            <th>Username</th>
            <th>{metricNameDisplay}</th>
          </tr>
        </thead>
        <tbody>
          {topChannelsData.data.map((channel, index) => (
            <tr key={channel.channel_id}>
              <td>{index + 1}</td>
              <td>{channel.channel_title}</td>
              <td>{channel.channel_username ? `@${channel.channel_username}` : '-'}</td>
              <td>{channel.metric_value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default TopChannels;