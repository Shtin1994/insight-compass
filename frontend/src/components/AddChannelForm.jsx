// frontend/src/components/AddChannelForm.jsx
import React, { useState } from 'react';

function AddChannelForm({ onAddChannel, isLoading }) {
  const [identifier, setIdentifier] = useState('');
  const [error, setError] = useState(''); // Для ошибок валидации или от API

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(''); // Сбрасываем предыдущие ошибки
    if (!identifier.trim()) {
      setError('Идентификатор канала не может быть пустым.');
      return;
    }
    try {
      await onAddChannel(identifier);
      setIdentifier(''); // Очищаем поле после успешного добавления
    } catch (err) {
      setError(err.message || 'Не удалось добавить канал.');
    }
  };

  return (
    <form onSubmit={handleSubmit} style={{ marginBottom: '20px' }}>
      <h3>Добавить новый канал</h3>
      <div>
        <input
          type="text"
          value={identifier}
          onChange={(e) => setIdentifier(e.target.value)}
          placeholder="Username или URL канала (e.g., telegramtips или t.me/telegramtips)"
          style={{ width: '300px', marginRight: '10px', padding: '8px' }}
          disabled={isLoading}
        />
        <button type="submit" disabled={isLoading}>
          {isLoading ? 'Добавление...' : 'Добавить'}
        </button>
      </div>
      {error && <p style={{ color: 'red', marginTop: '5px' }}>{error}</p>}
    </form>
  );
}

export default AddChannelForm;