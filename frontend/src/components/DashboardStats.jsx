// frontend/src/components/DashboardStats.jsx
import React, { useState, useEffect } from 'react';
import { fetchDashboardStatsAPI } from '../services/apiService'; // Мы создадим эту функцию в apiService.js

function DashboardStats() {
  const [stats, setStats] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const loadStats = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const data = await fetchDashboardStatsAPI();
        setStats(data);
      } catch (err) {
        setError(err.message);
        setStats(null);
      } finally {
        setIsLoading(false);
      }
    };
    loadStats();
  }, []); // Пустой массив зависимостей - загружаем один раз при монтировании

  if (isLoading) {
    return (
      <div className="spinner-container">
        <div className="spinner"></div>
      </div>
    );
  }

  if (error) {
    return <p>Ошибка загрузки статистики: {error}</p>;
  }

  if (!stats) {
    return <p>Нет данных для отображения статистики.</p>;
  }

  const statItemStyle = {
    border: '1px solid #eee',
    padding: '15px',
    margin: '10px',
    borderRadius: '5px',
    backgroundColor: '#f9f9f9',
    textAlign: 'center',
    minWidth: '180px'
  };

  const statValueStyle = {
    fontSize: '2em',
    fontWeight: 'bold',
    color: '#333',
    display: 'block'
  };
  
  const statLabelStyle = {
    fontSize: '0.9em',
    color: '#666',
    marginTop: '5px'
  };

  return (
    <div className="dashboard-stats" style={{ marginBottom: '30px' }}>
      <h3>Общая статистика</h3>
      <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-around' }}>
        <div style={statItemStyle}>
          <span style={statValueStyle}>{stats.total_posts_all_time}</span>
          <div style={statLabelStyle}>Всего постов</div>
        </div>
        <div style={statItemStyle}>
          <span style={statValueStyle}>{stats.total_comments_all_time}</span>
          <div style={statLabelStyle}>Всего комментариев</div>
        </div>
        <div style={statItemStyle}>
          <span style={statValueStyle}>{stats.posts_last_7_days}</span>
          <div style={statLabelStyle}>Постов за 7 дней</div>
        </div>
        <div style={statItemStyle}>
          <span style={statValueStyle}>{stats.comments_last_7_days}</span>
          <div style={statLabelStyle}>Комментариев за 7 дней</div>
        </div>
        <div style={statItemStyle}>
          <span style={statValueStyle}>{stats.channels_monitoring_count}</span>
          <div style={statLabelStyle}>Каналов на мониторинге</div>
        </div>
      </div>
    </div>
  );
}

export default DashboardStats;