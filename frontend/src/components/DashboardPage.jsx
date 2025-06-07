// frontend/src/components/DashboardPage.jsx
import React from 'react';
import DashboardStats from './DashboardStats'; 
import ActivityChart from './ActivityChart';
import TopChannels from './TopChannels';
import SentimentPieChart from './SentimentPieChart';
import CommentInsightsWidget from './CommentInsightsWidget';
import InsightTrendChart from './InsightTrendChart'; 
// --- НАЧАЛО: Импорт нового компонента NLQ ---
import NLQueryInterface from './NLQueryInterface'; 
// --- КОНЕЦ: Импорт нового компонента NLQ ---

function DashboardPage() { 
  return (
    <div className="dashboard-page p-5 space-y-8 bg-gray-100 min-h-screen"> {/* Увеличил space-y */}
      <h1 className="text-3xl font-bold text-gray-800 mb-8 text-center sm:text-left">Дашборд - Аналитика</h1>
      
      <DashboardStats />

      {/* --- НАЧАЛО: Добавление компонента NLQ --- */}
      <NLQueryInterface />
      {/* --- КОНЕЦ: Добавление компонента NLQ --- */}

      <div className="mt-8">
        <CommentInsightsWidget />
      </div>
      
      <div className="mt-8">
        <InsightTrendChart />
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
        <ActivityChart />
        <SentimentPieChart /> 
      </div>
      
      <div className="mt-8">
        <TopChannels />
      </div>
      
    </div>
  );
}

export default DashboardPage;