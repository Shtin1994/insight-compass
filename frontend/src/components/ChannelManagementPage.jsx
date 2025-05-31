// frontend/src/components/ChannelManagementPage.jsx
import React, { useState, useEffect, useCallback } from 'react';
import AddChannelForm from './AddChannelForm';
import ChannelList from './ChannelList';
import { fetchChannelsAPI, addChannelAPI, updateChannelAPI, deleteChannelAPI } from '../services/apiService';

function ChannelManagementPage() {
  const [channels, setChannels] = useState([]);
  const [isLoadingList, setIsLoadingList] = useState(true);
  const [listError, setListError] = useState(null);
  
  const [isAddingChannel, setIsAddingChannel] = useState(false);
  // Состояния для индикации загрузки для конкретных операций над каналами
  const [itemLoadingStates, setItemLoadingStates] = useState({ toggle: null, delete: null }); // { toggle: channelId, delete: channelId }

  // TODO: Добавить пагинацию, если каналов будет много
  // const [currentPage, setCurrentPage] = useState(1);
  // const [totalPages, setTotalPages] = useState(0);
  // const CHANNELS_PER_PAGE = 10; // Можно вынести в config.js

  const loadChannels = useCallback(async () => {
    setIsLoadingList(true);
    setListError(null);
    try {
      // Пока без пагинации для простоты, загружаем все каналы (или первые N, если API так настроен по умолчанию)
      const response = await fetchChannelsAPI(); // API вернет PaginatedChannelsResponse
      setChannels(response.channels || []);
      // setTotalPages(Math.ceil(response.total_channels / CHANNELS_PER_PAGE));
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
      loadChannels(); // Перезагружаем список после добавления
    } catch (err) {
      // Ошибка будет обработана в AddChannelForm
      console.error("Ошибка при добавлении канала:", err);
      throw err; // Пробрасываем ошибку, чтобы AddChannelForm ее показал
    } finally {
      setIsAddingChannel(false);
    }
  };

  const handleToggleActive = async (channelId, newActiveState) => {
    setItemLoadingStates(prev => ({ ...prev, toggle: channelId }));
    try {
      await updateChannelAPI(channelId, { is_active: newActiveState });
      // Обновляем состояние локально для мгновенного отклика или перезагружаем список
      setChannels(prevChannels =>
        prevChannels.map(ch =>
          ch.id === channelId ? { ...ch, is_active: newActiveState } : ch
        )
      );
    } catch (err) {
      alert(`Не удалось изменить статус канала: ${err.message}`);
      // Можно откатить изменение локально, если нужно, или перезагрузить loadChannels()
    } finally {
      setItemLoadingStates(prev => ({ ...prev, toggle: null }));
    }
  };

  const handleDeleteChannel = async (channelId) => {
    setItemLoadingStates(prev => ({ ...prev, delete: channelId }));
    try {
      await deleteChannelAPI(channelId);
      loadChannels(); // Перезагружаем список после удаления
    } catch (err) {
      alert(`Не удалось удалить канал: ${err.message}`);
    } finally {
      setItemLoadingStates(prev => ({ ...prev, delete: null }));
    }
  };

  return (
    <div className="channel-management-page" style={{ padding: '20px' }}>
      <h2>Управление Telegram-каналами</h2>
      <AddChannelForm onAddChannel={handleAddChannel} isLoading={isAddingChannel} />
      {listError && <p style={{ color: 'red' }}>{listError}</p>}
      <ChannelList
        channels={channels}
        onToggleActive={handleToggleActive}
        onDelete={handleDeleteChannel}
        isLoading={isLoadingList}
        loadingStates={itemLoadingStates}
      />
    </div>
  );
}

export default ChannelManagementPage;