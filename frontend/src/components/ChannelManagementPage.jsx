// frontend/src/components/ChannelManagementPage.jsx
import React, { useState, useEffect, useCallback } from 'react';
import AddChannelForm from './AddChannelForm';
import ChannelList from './ChannelList';
import AdvancedDataRefreshForm from './AdvancedDataRefreshForm'; // <--- НОВЫЙ ИМПОРТ
import { fetchChannelsAPI, addChannelAPI, updateChannelAPI, deleteChannelAPI } from '../services/apiService';
import './ChannelManagementPage.css'; // <--- Добавим файл стилей для страницы

function ChannelManagementPage() {
  const [channels, setChannels] = useState([]);
  const [isLoadingList, setIsLoadingList] = useState(true);
  const [listError, setListError] = useState(null);

  const [isAddingChannel, setIsAddingChannel] = useState(false);
  const [itemLoadingStates, setItemLoadingStates] = useState({ toggle: null, delete: null });

  const loadChannels = useCallback(async () => {
    setIsLoadingList(true);
    setListError(null);
    try {
      // Загружаем до 100 каналов, можно увеличить или добавить пагинацию для ChannelList
      const response = await fetchChannelsAPI(1, 100); 
      setChannels(response.channels || []);
    } catch (err) {
      setListError(err.message || 'Не удалось загрузить список каналов.');
      setChannels([]); // Убедимся, что channels - это массив при ошибке
    } finally {
      setIsLoadingList(false);
    }
  }, []);

  useEffect(() => {
    loadChannels();
  }, [loadChannels]);

  const handleAddChannel = async (identifier) => {
    setIsAddingChannel(true);
    try {
      await addChannelAPI(identifier);
      loadChannels();
    } catch (err) {
      console.error("Ошибка при добавлении канала:", err);
      // Ошибка будет отображена в AddChannelForm, если она оттуда пробрасывается и обрабатывается
      // Если нет, можно установить listError здесь или специфичное сообщение
      throw err; // Пробрасываем, чтобы AddChannelForm мог ее поймать и отобразить
    } finally {
      setIsAddingChannel(false);
    }
  };

  const handleToggleActive = async (channelId, newActiveState) => {
    setItemLoadingStates(prev => ({ ...prev, toggle: channelId }));
    try {
      await updateChannelAPI(channelId, { is_active: newActiveState });
      setChannels(prevChannels =>
        prevChannels.map(ch =>
          ch.id === channelId ? { ...ch, is_active: newActiveState } : ch
        )
      );
    } catch (err) {
      // Можно отобразить ошибку более заметно, например, через toast уведомление
      console.error(`Не удалось изменить статус канала ${channelId}:`, err);
      alert(`Не удалось изменить статус канала: ${err.message}`);
    } finally {
      setItemLoadingStates(prev => ({ ...prev, toggle: null }));
    }
  };

  const handleDeleteChannel = async (channelId) => {
    if (window.confirm('Вы уверены, что хотите деактивировать этот канал? Сбор данных по нему прекратится.')) {
        setItemLoadingStates(prev => ({ ...prev, delete: channelId }));
        try {
          await deleteChannelAPI(channelId);
          loadChannels(); 
        } catch (err) {
          console.error(`Не удалось деактивировать канал ${channelId}:`, err);
          alert(`Не удалось деактивировать канал: ${err.message}`);
        } finally {
          setItemLoadingStates(prev => ({ ...prev, delete: null }));
        }
    }
  };

  return (
    <div className="channel-management-page">
      <h2>Управление Telegram-каналами и данными</h2>
      
      <div className="page-section add-channel-section">
        <AddChannelForm onAddChannel={handleAddChannel} isLoading={isAddingChannel} />
      </div>

      <div className="page-section channel-list-section">
        <h3>Список отслеживаемых каналов</h3>
        {listError && <p className="error-message">{listError}</p>}
        <ChannelList
          channels={channels}
          onToggleActive={handleToggleActive}
          onDelete={handleDeleteChannel}
          isLoading={isLoadingList}
          loadingStates={itemLoadingStates}
        />
      </div>

      <div className="page-section advanced-tasks-section">
        <h3 className="section-title">Задачи по сбору данных</h3>
        
        {/* --- Форма для Кнопки 1: Сбор и обновление постов --- */}
        <div className="task-form-wrapper">
            <AdvancedDataRefreshForm formType="collectPosts" />
        </div>

        {/* --- Форма для Кнопки 2: Сбор и обновление комментариев --- */}
        <div className="task-form-wrapper">
            <AdvancedDataRefreshForm formType="collectComments" />
        </div>
      </div>

    </div>
  );
}

export default ChannelManagementPage;