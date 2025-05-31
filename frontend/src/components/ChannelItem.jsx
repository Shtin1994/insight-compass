// frontend/src/components/ChannelItem.jsx
import React from 'react';

function ChannelItem({ channel, onToggleActive, onDelete, isLoading }) {
  const handleToggle = () => {
    onToggleActive(channel.id, !channel.is_active);
  };

  const handleDelete = () => {
    if (window.confirm(`Вы уверены, что хотите удалить (деактивировать) канал "${channel.title}"? Собранные данные останутся.`)) {
      onDelete(channel.id);
    }
  };

  return (
    <tr>
      <td>{channel.id}</td>
      <td>{channel.title}</td>
      <td>{channel.username ? `@${channel.username}` : '-'}</td>
      <td>
        <button onClick={handleToggle} disabled={isLoading.toggle === channel.id || isLoading.delete === channel.id}>
          {isLoading.toggle === channel.id ? 'Обновление...' : (channel.is_active ? 'Активен' : 'Неактивен')}
        </button>
      </td>
      <td>
        <button onClick={handleDelete} disabled={isLoading.delete === channel.id || isLoading.toggle === channel.id} style={{backgroundColor: '#f44336', color: 'white'}}>
          {isLoading.delete === channel.id ? 'Удаление...' : 'Удалить'}
        </button>
      </td>
    </tr>
  );
}

export default ChannelItem;