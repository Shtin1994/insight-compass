// frontend/src/components/DashboardPage.jsx
import React from 'react';
import DashboardStats from './DashboardStats';
import ActivityChart from './ActivityChart';
import TopChannels from './TopChannels';
import SentimentPieChart from './SentimentPieChart'; // Убедитесь, что импорт есть

// Только ОДНО определение функции DashboardPage
function DashboardPage() { 
  return (
    <div className="dashboard-page" style={{padding: '20px'}}>
      <h2>Дашборд - Аналитика</h2>
      
      <DashboardStats />
      <ActivityChart />
      <TopChannels />
      <SentimentPieChart /> 
      
    </div>
  );
}

export default DashboardPage; // И один экспорт по умолчанию