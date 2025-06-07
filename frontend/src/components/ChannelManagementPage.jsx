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
      const response = await fetchChannelsAPI(1, 100); // Загружаем до 100 каналов
      setChannels(response.channels || []);
    } catch (err) {
      setListError(err.message || 'Не удалось загрузить список каналов.');
      setChannels([]);
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
      throw err; 
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
      alert(`Не удалось изменить статус канала: ${err.message}`);
    } finally {
      setItemLoadingStates(prev => ({ ...prev, toggle: null }));
    }
  };

  const handleDeleteChannel = async (channelId) => {
    // В текущей реализации API deleteChannelAPI деактивирует канал, а не удаляет.
    // Если нужно полное удаление из БД, API и логика здесь должны быть другими.
    // Пока что это будет работать как деактивация.
    if (window.confirm('Вы уверены, что хотите деактивировать этот канал? Сбор данных по нему прекратится.')) {
        setItemLoadingStates(prev => ({ ...prev, delete: channelId }));
        try {
          await deleteChannelAPI(channelId); // Этот эндпоинт деактивирует канал
          loadChannels(); // Перезагружаем список, чтобы отразить статус is_active=false
        } catch (err) {
          alert(`Не удалось деактивировать канал: ${err.message}`);
        } finally {
          setItemLoadingStates(prev => ({ ...prev, delete: null }));
        }
    }
  };

  return (
    <div className="channel-management-page">
      <h2>Управление Telegram-каналами</h2>
      
      <div className="page-section add-channel-section">
        <AddChannelForm onAddChannel={handleAddChannel} isLoading={isAddingChannel} />
      </div>

      <div className="page-section channel-list-section">
        <h3>Список отслеживаемых каналов</h3>
        {listError && <p className="error-message">{listError}</p>}
        <ChannelList
          channels={channels}
          onToggleActive={handleToggleActive}
          onDelete={handleDeleteChannel} // Переименовал проп для ясности, что это деактивация
          isLoading={isLoadingList}
          loadingStates={itemLoadingStates}
        />
      </div>

      {/* --- НАЧАЛО: Интеграция формы продвинутого обновления --- */}
      <div className="page-section advanced-refresh-section">
        <AdvancedDataRefreshForm />
      </div>
      {/* --- КОНЕЦ: Интеграция формы продвинутого обновления --- */}

    </div>
  );
}

export default ChannelManagementPage;