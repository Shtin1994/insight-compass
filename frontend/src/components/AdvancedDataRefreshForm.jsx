// frontend/src/components/AdvancedDataRefreshForm.jsx
import React, { useState, useEffect } from 'react';
import { runAdvancedDataRefreshAPI, fetchChannelsAPI } from '../services/apiService';
import './AdvancedDataRefreshForm.css'; // Создадим этот файл для стилей позже

// Значения по умолчанию из Pydantic схемы (для удобства)
const POST_REFRESH_MODES = [
  { value: 'new_only', label: 'Только новые посты (с последнего ID)' },
  { value: 'last_n_days', label: 'Посты за последние N дней' },
  { value: 'since_date', label: 'Посты с указанной даты' },
];

const COMMENT_REFRESH_MODES = [
  { value: 'new_posts_only', label: 'Только для абсолютно новых постов' },
  { value: 'add_new_to_existing', label: 'Дособрать новые к существующим постам в выборке' },
  // { value: 'rebuild_all', label: 'Полностью пересобрать все комментарии (Опасно!)' }, // Пока не реализуем
];

const AdvancedDataRefreshForm = () => {
  const [formData, setFormData] = useState({
    channel_ids: [], // Будет массив ID каналов
    post_refresh_mode: 'new_only',
    post_refresh_days: 7,
    post_refresh_start_date_str: '', // YYYY-MM-DD
    post_limit_per_channel: 100,
    update_existing_posts_info: false,
    comment_refresh_mode: 'add_new_to_existing',
    comment_limit_per_post: 200, // Значение из settings.COMMENT_FETCH_LIMIT (если нужно синхронизировать)
    analyze_new_comments: true,
  });

  const [availableChannels, setAvailableChannels] = useState([]);
  const [selectedChannelObjects, setSelectedChannelObjects] = useState([]);
  const [loadingChannels, setLoadingChannels] = useState(false);
  const [loadingSubmit, setLoadingSubmit] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    const loadChannels = async () => {
      setLoadingChannels(true);
      try {
        // Загружаем все каналы (можно добавить пагинацию, если их очень много)
        // Для простоты, пока загружаем первые 100
        const response = await fetchChannelsAPI(1, 100); 
        if (response && response.channels) {
          setAvailableChannels(response.channels.filter(ch => ch.is_active)); // Только активные
        }
      } catch (err) {
        console.error("Ошибка загрузки каналов:", err);
        setError("Не удалось загрузить список каналов.");
      } finally {
        setLoadingChannels(false);
      }
    };
    loadChannels();
  }, []);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : (type === 'number' ? parseInt(value, 10) : value),
    }));
  };

  const handleChannelChange = (e) => {
    const selectedOptions = Array.from(e.target.selectedOptions, option => {
        const channel = availableChannels.find(ch => ch.id === parseInt(option.value));
        return channel ? { id: channel.id, title: channel.title } : null;
    }).filter(Boolean); // Убираем null, если канал не найден (маловероятно)
    
    setSelectedChannelObjects(selectedOptions);
    setFormData(prev => ({
        ...prev,
        channel_ids: selectedOptions.map(ch => ch.id)
    }));
  };
  
  const handleSelectAllChannels = () => {
    setSelectedChannelObjects(availableChannels);
    setFormData(prev => ({
        ...prev,
        channel_ids: availableChannels.map(ch => ch.id)
    }));
  };

  const handleDeselectAllChannels = () => {
    setSelectedChannelObjects([]);
    setFormData(prev => ({
        ...prev,
        channel_ids: []
    }));
  };


  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoadingSubmit(true);
    setMessage('');
    setError('');

    const payload = { ...formData };
    // Если список channel_ids пуст, бэкенд обработает это как "все активные"
    // Но если мы хотим явно передать null, если ничего не выбрано (и это не "все активные"),
    // то можно добавить условие:
    // if (payload.channel_ids && payload.channel_ids.length === 0) {
    //   payload.channel_ids = null; // или оставить пустым массивом, в зависимости от логики бэкенда
    // }

    // Валидация зависимых полей перед отправкой (хотя Pydantic на бэке тоже проверит)
    if (payload.post_refresh_mode === 'last_n_days' && (!payload.post_refresh_days || payload.post_refresh_days <= 0)) {
      setError('Для режима "Посты за N дней" необходимо указать корректное количество дней.');
      setLoadingSubmit(false);
      return;
    }
    if (payload.post_refresh_mode === 'since_date' && !payload.post_refresh_start_date_str) {
      setError('Для режима "Посты с даты" необходимо указать дату.');
      setLoadingSubmit(false);
      return;
    }
    if (payload.post_refresh_start_date_str === '') {
        payload.post_refresh_start_date_str = null; // Отправляем null если пусто
    }


    try {
      const response = await runAdvancedDataRefreshAPI(payload);
      setMessage(`Задача успешно запущена! ID Задачи: ${response.task_id}. Детали: ${JSON.stringify(response.details)}`);
      // Можно сбросить форму или часть полей при успехе
    } catch (err) {
      console.error("Ошибка запуска задачи обновления:", err);
      setError(err.message || "Произошла ошибка при запуске задачи.");
    } finally {
      setLoadingSubmit(false);
    }
  };

  return (
    <div className="advanced-refresh-form-container">
      <h3>Продвинутое обновление данных</h3>
      {error && <p className="error-message">{error}</p>}
      {message && <p className="success-message">{message}</p>}
      
      <form onSubmit={handleSubmit}>
        <div className="form-section">
          <h4>Выбор каналов</h4>
          <label htmlFor="channel_ids">Каналы для обновления (Ctrl/Cmd + Click для множественного выбора):</label>
          {loadingChannels ? <p>Загрузка каналов...</p> : (
            <>
              <select
                id="channel_ids"
                name="channel_ids"
                multiple
                value={formData.channel_ids} // value для multiple select это массив ID
                onChange={handleChannelChange}
                size="5" // Количество видимых опций
                className="form-control"
              >
                {availableChannels.map((channel) => (
                  <option key={channel.id} value={channel.id}>
                    {channel.title} (@{channel.username || channel.id})
                  </option>
                ))}
              </select>
              <div className="channel-select-buttons">
                <button type="button" onClick={handleSelectAllChannels} disabled={loadingChannels}>Выбрать все</button>
                <button type="button" onClick={handleDeselectAllChannels} disabled={loadingChannels}>Снять выделение</button>
              </div>
              <div>
                <strong>Выбрано каналов: {selectedChannelObjects.length}</strong>
                {selectedChannelObjects.length > 0 && (
                    <ul>
                        {selectedChannelObjects.slice(0, 5).map(ch => <li key={ch.id}>{ch.title}</li>)}
                        {selectedChannelObjects.length > 5 && <li>...и еще {selectedChannelObjects.length - 5}</li>}
                    </ul>
                )}
                {selectedChannelObjects.length === 0 && <p><small>(Если ни один канал не выбран, будут обработаны все активные каналы)</small></p>}
              </div>
            </>
          )}
        </div>

        <div className="form-section">
          <h4>Обновление постов</h4>
          <label htmlFor="post_refresh_mode">Режим обновления постов:</label>
          <select 
            id="post_refresh_mode" 
            name="post_refresh_mode" 
            value={formData.post_refresh_mode} 
            onChange={handleChange}
            className="form-control"
          >
            {POST_REFRESH_MODES.map(mode => (
              <option key={mode.value} value={mode.value}>{mode.label}</option>
            ))}
          </select>

          {formData.post_refresh_mode === 'last_n_days' && (
            <div>
              <label htmlFor="post_refresh_days">Количество дней (для "Посты за N дней"):</label>
              <input 
                type="number" 
                id="post_refresh_days" 
                name="post_refresh_days" 
                value={formData.post_refresh_days} 
                onChange={handleChange} 
                min="1" 
                max="365"
                className="form-control"
              />
            </div>
          )}

          {formData.post_refresh_mode === 'since_date' && (
            <div>
              <label htmlFor="post_refresh_start_date_str">Дата начала (ГГГГ-ММ-ДД):</label>
              <input 
                type="date" 
                id="post_refresh_start_date_str" 
                name="post_refresh_start_date_str" 
                value={formData.post_refresh_start_date_str} 
                onChange={handleChange}
                className="form-control"
              />
            </div>
          )}
          
          {(formData.post_refresh_mode === 'last_n_days' || formData.post_refresh_mode === 'since_date') && (
             <div>
                <label htmlFor="post_limit_per_channel">Лимит постов на канал (для режимов "N дней" / "С даты"):</label>
                <input 
                    type="number" 
                    id="post_limit_per_channel" 
                    name="post_limit_per_channel" 
                    value={formData.post_limit_per_channel} 
                    onChange={handleChange} 
                    min="10" 
                    max="5000"
                    className="form-control"
                />
            </div>
          )}


          <div>
            <input 
              type="checkbox" 
              id="update_existing_posts_info" 
              name="update_existing_posts_info" 
              checked={formData.update_existing_posts_info} 
              onChange={handleChange} 
            />
            <label htmlFor="update_existing_posts_info">Обновлять инфо (просмотры, реакции) для существующих постов?</label>
          </div>
        </div>

        <div className="form-section">
          <h4>Обновление комментариев</h4>
          <label htmlFor="comment_refresh_mode">Режим обновления комментариев:</label>
          <select 
            id="comment_refresh_mode" 
            name="comment_refresh_mode" 
            value={formData.comment_refresh_mode} 
            onChange={handleChange}
            className="form-control"
          >
            {COMMENT_REFRESH_MODES.map(mode => (
              <option key={mode.value} value={mode.value}>{mode.label}</option>
            ))}
          </select>

          <div>
            <label htmlFor="comment_limit_per_post">Лимит комментариев на пост:</label>
            <input 
              type="number" 
              id="comment_limit_per_post" 
              name="comment_limit_per_post" 
              value={formData.comment_limit_per_post} 
              onChange={handleChange} 
              min="10" 
              max="1000"
              className="form-control"
            />
          </div>
        </div>

        <div className="form-section">
          <h4>AI-Анализ</h4>
          <div>
            <input 
              type="checkbox" 
              id="analyze_new_comments" 
              name="analyze_new_comments" 
              checked={formData.analyze_new_comments} 
              onChange={handleChange} 
            />
            <label htmlFor="analyze_new_comments">Запускать AI-анализ для новых/обновленных комментариев?</label>
          </div>
        </div>
        
        <button type="submit" disabled={loadingSubmit || loadingChannels} className="submit-button">
          {loadingSubmit ? 'Запуск задачи...' : 'Запустить обновление'}
        </button>
      </form>
    </div>
  );
};

export default AdvancedDataRefreshForm;