// frontend/src/components/AdvancedDataRefreshForm.jsx
import React, { useState, useEffect, useCallback } from 'react';
import { runAdvancedDataRefreshAPI, fetchChannelsAPI, fetchTaskStatusAPI } from '../services/apiService';
import './AdvancedDataRefreshForm.css'; // Убедитесь, что этот файл существует или создайте его

const POST_REFRESH_MODES = [
  { value: 'new_only', label: 'Только новые посты (с последнего ID)' },
  { value: 'last_n_days', label: 'Посты за последние N дней' },
  { value: 'since_date', label: 'Посты с указанной даты' },
  { value: 'update_stats_only', label: 'Только обновить статистику (без загрузки новых постов)' }, // Добавлен для сбора комментов
];

const COMMENT_REFRESH_MODES = [
  { value: 'new_posts_only', label: 'Только для абсолютно новых постов' },
  { value: 'add_new_to_existing', label: 'Дособрать новые к существующим постам в выборке' },
  { value: 'do_not_refresh', label: 'Не обновлять комментарии' }, // Добавлен для сбора постов
];

const AdvancedDataRefreshForm = ({ formType = 'collectPosts' }) => {
  const getDefaultFormData = useCallback(() => ({
    channel_ids: [],
    post_refresh_mode: formType === 'collectPosts' ? 'new_only' : 'update_stats_only',
    post_refresh_days: 7,
    post_refresh_start_date_str: '',
    post_limit_per_channel: formType === 'collectPosts' ? 100 : 50, // Разные дефолты
    update_existing_posts_info: formType === 'collectPosts', // По умолчанию true для постов
    comment_refresh_mode: formType === 'collectComments' ? 'add_new_to_existing' : 'do_not_refresh',
    comment_limit_per_post: 200,
    analyze_new_comments: formType === 'collectComments', // По умолчанию true для комментов
  }), [formType]);

  const [formData, setFormData] = useState(getDefaultFormData());

  const [availableChannels, setAvailableChannels] = useState([]);
  const [selectedChannelObjects, setSelectedChannelObjects] = useState([]);
  const [loadingChannels, setLoadingChannels] = useState(false);
  const [submittingTask, setSubmittingTask] = useState(false);

  const [activeTaskId, setActiveTaskId] = useState(null);
  const [taskStatus, setTaskStatus] = useState('');
  const [taskProgress, setTaskProgress] = useState(0);
  const [taskMeta, setTaskMeta] = useState(null);

  const [formMessage, setFormMessage] = useState('');
  const [formError, setFormError] = useState('');

  // Сброс состояния формы и задачи при смене formType
  useEffect(() => {
    setFormData(getDefaultFormData());
    setActiveTaskId(null);
    setTaskStatus('');
    setTaskProgress(0);
    setTaskMeta(null);
    setFormMessage('');
    setFormError('');
    setSelectedChannelObjects([]);
  }, [formType, getDefaultFormData]);


  useEffect(() => {
    const loadChannels = async () => {
      setLoadingChannels(true);
      setFormError(''); // Сброс ошибки перед загрузкой
      try {
        const response = await fetchChannelsAPI(1, 100); // TODO: implement full pagination if >100 channels
        if (response && response.channels) {
          setAvailableChannels(response.channels.filter(ch => ch.is_active));
        } else {
          setAvailableChannels([]); // Установить пустой массив, если каналы не пришли
        }
      } catch (err) {
        console.error("Ошибка загрузки каналов:", err);
        setFormError("Не удалось загрузить список каналов. " + err.message);
        setAvailableChannels([]); // Убедиться, что пустой при ошибке
      } finally {
        setLoadingChannels(false);
      }
    };
    loadChannels();
  }, []);

  useEffect(() => {
    let intervalId = null;

    const fetchStatus = async () => {
      if (!activeTaskId) return;

      try {
        const statusResponse = await fetchTaskStatusAPI(activeTaskId);
        setTaskStatus(statusResponse.status);

        const metaProgress = statusResponse.meta?.progress;
        const currentProgress = typeof metaProgress === 'number' ? metaProgress : (statusResponse.status === 'SUCCESS' || statusResponse.status === 'FAILURE' ? 100 : 0);
        
        setTaskProgress(currentProgress);
        setTaskMeta(statusResponse.meta || null); // Сохраняем все метаданные

        if (statusResponse.status === 'SUCCESS' || statusResponse.status === 'FAILURE') {
          if (intervalId) clearInterval(intervalId);
          // Сообщение об успехе/ошибке будет отображено в секции статуса задачи
        }
      } catch (err) {
        console.error("Ошибка получения статуса задачи:", err);
        // Не перезаписываем formError, если это ошибка опроса, а не запуска
        setTaskStatus('ERROR_FETCHING_STATUS'); 
        if (intervalId) clearInterval(intervalId);
      }
    };

    if (activeTaskId && taskStatus !== 'SUCCESS' && taskStatus !== 'FAILURE' && taskStatus !== 'ERROR_FETCHING_STATUS') {
      fetchStatus(); 
      intervalId = setInterval(fetchStatus, 3000); // Уменьшил интервал
    }

    return () => {
      if (intervalId) clearInterval(intervalId);
    };
  }, [activeTaskId, taskStatus]);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : (type === 'number' ? (value === '' ? '' : parseInt(value, 10)) : value),
    }));
  };

  const handleChannelChange = (e) => {
    const selectedOptions = Array.from(e.target.selectedOptions, option => {
        const channelId = parseInt(option.value);
        const channel = availableChannels.find(ch => ch.id === channelId);
        return channel ? { id: channel.id, title: channel.title } : null;
    }).filter(Boolean);
    setSelectedChannelObjects(selectedOptions);
    setFormData(prev => ({ ...prev, channel_ids: selectedOptions.map(ch => ch.id) }));
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
    setSubmittingTask(true);
    setFormMessage('');
    setFormError('');
    setActiveTaskId(null);
    setTaskStatus('');
    setTaskProgress(0);
    setTaskMeta(null);

    const payload = { ...formData };
    if (payload.channel_ids && payload.channel_ids.length === 0) {
      payload.channel_ids = null; 
    }
    if (payload.post_refresh_start_date_str === '') {
        payload.post_refresh_start_date_str = null;
    }
    
    // Приведение пустых числовых полей к null или дефолтным значениям бэкенда
    for (const key of ['post_refresh_days', 'post_limit_per_channel', 'comment_limit_per_post']) {
        if (payload[key] === '' || payload[key] === null || isNaN(payload[key])) {
             // Бэкенд должен иметь дефолты или обрабатывать null для этих числовых полей
             // Если мы хотим передавать null для пустых значений:
             payload[key] = null; 
        }
    }


    // Принудительная установка режимов в зависимости от formType
    if (formType === 'collectPosts') {
      payload.comment_refresh_mode = 'do_not_refresh';
      payload.analyze_new_comments = false; // Обычно не анализируем комменты при сборе постов
    } else if (formType === 'collectComments') {
      payload.post_refresh_mode = 'update_stats_only';
      if (payload.comment_refresh_mode === 'do_not_refresh') {
          setFormError('Для сбора комментариев выберите подходящий режим обновления комментариев (не "Не обновлять").');
          setSubmittingTask(false);
          return;
      }
    }

    // Валидация
    if (payload.post_refresh_mode === 'last_n_days' && (payload.post_refresh_days === null || payload.post_refresh_days <= 0)) {
      setFormError('Для режима "Посты за N дней" необходимо указать корректное количество дней (больше 0).');
      setSubmittingTask(false); return;
    }
    if (payload.post_refresh_mode === 'since_date' && !payload.post_refresh_start_date_str) {
      setFormError('Для режима "Посты с даты" необходимо указать дату.');
      setSubmittingTask(false); return;
    }

    try {
      console.log("Отправка payload на бэкенд:", payload);
      const response = await runAdvancedDataRefreshAPI(payload);
      if (response && response.task_id) {
        setFormMessage(''); // Очищаем общее сообщение, статус будет в блоке задачи
        setActiveTaskId(response.task_id);
        setTaskStatus('PENDING');
      } else {
        setFormError('Не удалось получить ID задачи от бэкенда. Ответ: ' + JSON.stringify(response));
        console.error("Ответ от runAdvancedDataRefreshAPI не содержит task_id:", response);
      }
    } catch (err) {
      console.error("Ошибка запуска задачи обновления:", err);
      setFormError(err.message || "Произошла ошибка при запуске задачи.");
    } finally {
      setSubmittingTask(false);
    }
  };
  
  const isTaskActive = activeTaskId && taskStatus !== 'SUCCESS' && taskStatus !== 'FAILURE' && taskStatus !== 'ERROR_FETCHING_STATUS';

  return (
    <div className="advanced-refresh-form-container">
      <h3>
        {formType === 'collectPosts' ? 'Сбор и обновление постов' : 'Сбор и обновление комментариев'}
      </h3>
      {formError && !activeTaskId && <p className="error-message">{formError}</p>} {/* Показываем ошибку формы только если нет активной задачи */}
      {formMessage && !activeTaskId && <p className="success-message">{formMessage}</p>}

      {activeTaskId && (
        <div className="task-status-section">
          <h4>Статус выполнения задачи (ID: {activeTaskId})</h4>
          <p>Общий статус: <strong>{taskStatus || 'Ожидание...'}</strong></p>
          {taskMeta && taskMeta.current_step && (
            <p>Текущий шаг: {taskMeta.current_step}</p>
          )}
          {isTaskActive && (
            <div className="progress-bar-container">
              <div
                className="progress-bar"
                style={{ width: `${taskProgress}%` }}
              >
                {taskProgress > 5 ? `${taskProgress}%` : ''}
              </div>
            </div>
          )}
          {taskMeta && typeof taskMeta.processed_count === 'number' && typeof taskMeta.total_to_process === 'number' && taskMeta.total_to_process > 0 && (
             <p>Обработано: {taskMeta.processed_count} / {taskMeta.total_to_process}</p>
          )}
           {taskStatus === 'SUCCESS' && taskMeta?.result_summary && (
             <p className="success-message task-result-summary">Результат: {taskMeta.result_summary}</p>
           )}
           {taskStatus === 'FAILURE' && (
             <p className="error-message task-result-summary">
                Ошибка задачи: {taskMeta?.error || (taskMeta?.raw_info_on_failure ? JSON.stringify(taskMeta.raw_info_on_failure) : 'Детали неизвестны')}
             </p>
           )}
           {taskStatus === 'ERROR_FETCHING_STATUS' && (
            <p className="error-message task-result-summary">Не удалось обновить статус задачи. Проверьте консоль.</p>
           )}
        </div>
      )}
      
      <form onSubmit={handleSubmit}>
        <div className="form-section">
          <h4>Выбор каналов</h4>
          <label htmlFor="channel_ids">Каналы для обновления (оставьте пустым для всех активных):</label>
          {loadingChannels ? <p>Загрузка каналов...</p> : (
            availableChannels.length > 0 ? (
            <>
              <select
                id="channel_ids"
                name="channel_ids"
                multiple
                value={formData.channel_ids}
                onChange={handleChannelChange}
                size={Math.min(availableChannels.length, 5)} 
                className="form-control"
                disabled={isTaskActive || submittingTask}
              >
                {availableChannels.map((channel) => (
                  <option key={channel.id} value={channel.id}>
                    {channel.title} (@{channel.username || `ID:${channel.id}`})
                  </option>
                ))}
              </select>
              <div className="channel-select-buttons">
                <button type="button" onClick={handleSelectAllChannels} disabled={loadingChannels || isTaskActive || submittingTask}>Выбрать все</button>
                <button type="button" onClick={handleDeselectAllChannels} disabled={loadingChannels || isTaskActive || submittingTask}>Снять выделение</button>
              </div>
              <div>
                <strong>Выбрано каналов: {selectedChannelObjects.length}</strong>
                {selectedChannelObjects.length > 0 && (
                    <ul className="selected-channels-list">
                        {selectedChannelObjects.slice(0, 3).map(ch => <li key={ch.id}>{ch.title}</li>)}
                        {selectedChannelObjects.length > 3 && <li>...и еще {selectedChannelObjects.length - 3}</li>}
                    </ul>
                )}
                {selectedChannelObjects.length === 0 && <p><small>(Если ни один канал не выбран, будут обработаны все активные каналы)</small></p>}
              </div>
            </>
            ) : <p>Нет доступных активных каналов для выбора или произошла ошибка загрузки.</p>
          )}
        </div>
        
        {/* Секция Обновление постов */}
        <div className="form-section">
            <h4>
              {formType === 'collectPosts' ? 'Параметры сбора постов' : 'Параметры выбора постов (для сбора комментариев)'}
            </h4>
            {formType === 'collectComments' && <p><small>Будет использован режим <code>update_stats_only</code> для постов. Ниже укажите, для каких постов собрать комментарии.</small></p>}

            <label htmlFor="post_refresh_mode">Режим обновления/выбора постов:</label>
            <select 
              id="post_refresh_mode" name="post_refresh_mode" 
              value={formData.post_refresh_mode} onChange={handleChange} 
              className="form-control"
              disabled={isTaskActive || submittingTask || formType === 'collectComments'}
            >
              {POST_REFRESH_MODES.map(mode => (<option key={mode.value} value={mode.value}>{mode.label}</option>))}
            </select>

            {(formData.post_refresh_mode === 'last_n_days' || (formType === 'collectComments' && formData.post_refresh_days)) && (
              <div>
                <label htmlFor="post_refresh_days">Количество дней (для "Посты за N дней"):</label>
                <input type="number" id="post_refresh_days" name="post_refresh_days" value={formData.post_refresh_days} onChange={handleChange} min="1" max="365" className="form-control" disabled={isTaskActive || submittingTask}/>
              </div>
            )}

            {(formData.post_refresh_mode === 'since_date' || (formType === 'collectComments' && formData.post_refresh_start_date_str)) && (
              <div>
                <label htmlFor="post_refresh_start_date_str">Дата начала (ГГГГ-ММ-ДД):</label>
                <input type="date" id="post_refresh_start_date_str" name="post_refresh_start_date_str" value={formData.post_refresh_start_date_str} onChange={handleChange} className="form-control" disabled={isTaskActive || submittingTask}/>
              </div>
            )}
            
            {/* Лимит постов показываем всегда, если это не new_only для постов */}
            {!(formType === 'collectPosts' && formData.post_refresh_mode === 'new_only') && (
              <div>
                  <label htmlFor="post_limit_per_channel">Лимит постов на канал (для режимов "N дней" / "С даты" / Сбора комментов):</label>
                  <input type="number" id="post_limit_per_channel" name="post_limit_per_channel" value={formData.post_limit_per_channel} onChange={handleChange} min="1" max="10000" className="form-control" disabled={isTaskActive || submittingTask}/>
              </div>
            )}

            <div>
              <input type="checkbox" id="update_existing_posts_info" name="update_existing_posts_info" checked={formData.update_existing_posts_info} onChange={handleChange} disabled={isTaskActive || submittingTask}/>
              <label htmlFor="update_existing_posts_info">Обновлять инфо (просмотры, реакции) для существующих/выбранных постов?</label>
            </div>
        </div>

        {/* Секция Обновление комментариев */}
        <div className="form-section">
          <h4>
            {formType === 'collectComments' ? 'Параметры сбора комментариев' : 'Сбор комментариев (для собранных постов)'}
          </h4>
          <label htmlFor="comment_refresh_mode">Режим обновления комментариев:</label>
          <select
            id="comment_refresh_mode" name="comment_refresh_mode"
            value={formData.comment_refresh_mode} onChange={handleChange}
            className="form-control"
            disabled={isTaskActive || submittingTask || formType === 'collectPosts'}
          >
            {COMMENT_REFRESH_MODES.map(mode => (
              <option key={mode.value} value={mode.value}>{mode.label}</option>
            ))}
          </select>

          {formData.comment_refresh_mode !== 'do_not_refresh' && ( // Показываем только если выбран режим сбора комментов
            <>
              <div>
                <label htmlFor="comment_limit_per_post">Лимит комментариев на пост:</label>
                <input type="number" id="comment_limit_per_post" name="comment_limit_per_post" value={formData.comment_limit_per_post} onChange={handleChange} min="1" max="5000" className="form-control" disabled={isTaskActive || submittingTask}/>
              </div>
              
              <div className="form-section"> {/* Вложенная секция для AI-анализа */}
                <h5>AI-Анализ комментариев</h5>
                <div>
                  <input type="checkbox" id="analyze_new_comments" name="analyze_new_comments" checked={formData.analyze_new_comments} onChange={handleChange} disabled={isTaskActive || submittingTask}/>
                  <label htmlFor="analyze_new_comments">Запускать AI-анализ для новых/обновленных комментариев?</label>
                </div>
              </div>
            </>
          )}
        </div>
        
        <button type="submit" disabled={submittingTask || loadingChannels || isTaskActive} className="submit-button">
          {submittingTask ? 'Запуск...' : (isTaskActive ? 'Задача выполняется...' : (formType === 'collectPosts' ? 'Запустить сбор постов' : 'Запустить сбор комментариев'))}
        </button>
      </form>
    </div>
  );
};

export default AdvancedDataRefreshForm;