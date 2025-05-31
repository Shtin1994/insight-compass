// frontend/src/components/ChannelList.jsx
import React from 'react';
import ChannelItem from './ChannelItem';
// Pagination компонент можно будет добавить позже, если каналов станет много

function ChannelList({ channels, onToggleActive, onDelete, isLoading, loadingStates }) {
  if (isLoading) {
    return (
      <div className="spinner-container" style={{minHeight: '100px'}}>
        <div className="spinner"></div>
      </div>
    );
  }

  if (!channels || channels.length === 0) {
    return <p>Нет отслеживаемых каналов. Добавьте первый канал.</p>;
  }

  return (
    <div className="channel-list">
      <h3>Список отслеживаемых каналов</h3>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Название</th>
            <th>Username</th>
            <th>Статус</th>
            <th>Действия</th>
          </tr>
        </thead>
        <tbody>
          {channels.map((channel) => (
            <ChannelItem
              key={channel.id}
              channel={channel}
              onToggleActive={onToggleActive}
              onDelete={onDelete}
              isLoading={loadingStates} // Передаем состояния загрузки для конкретной строки
            />
          ))}
        </tbody>
      </table>
      {/* Здесь можно будет добавить Pagination, если нужно */}
    </div>
  );
}

export default ChannelList;