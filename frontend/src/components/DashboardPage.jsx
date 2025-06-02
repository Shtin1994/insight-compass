// frontend/src/components/DashboardPage.jsx
import React from 'react';
// --- НАЧАЛО ИЗМЕНЕНИЙ: Исправлены пути импорта для плоской структуры ---
import DashboardStats from './DashboardStats'; 
import ActivityChart from './ActivityChart';
import TopChannels from './TopChannels';
import SentimentPieChart from './SentimentPieChart';
import CommentInsightsWidget from './CommentInsightsWidget'; // Все компоненты в той же папке
// --- КОНЕЦ ИЗМЕНЕНИЙ ---

function DashboardPage() { 
  return (
    <div className="dashboard-page p-5 space-y-6 bg-gray-50 min-h-screen">
      <h1 className="text-2xl font-semibold text-gray-800 mb-6">Дашборд - Аналитика</h1>
      
      <DashboardStats />

      <div className="mt-6">
        <CommentInsightsWidget />
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
        <ActivityChart />
        <SentimentPieChart /> 
      </div>
      
      <div className="mt-6">
        <TopChannels />
      </div>
      
    </div>
  );
}

export default DashboardPage;