// frontend/src/App.jsx

import React, { useState, useEffect } from 'react';
import './App.css';
import PostList from './components/PostList';
import CommentList from './components/CommentList';
import DashboardPage from './components/DashboardPage';
import ChannelManagementPage from './components/ChannelManagementPage';
import { fetchCommentsAPI } from './services/apiService';
import { COMMENTS_PER_PAGE } from './config';

function App() {
  const [selectedPostId, setSelectedPostId] = useState(null);
  const [comments, setComments] = useState([]);
  const [isLoadingComments, setIsLoadingComments] = useState(false);
  const [commentsError, setCommentsError] = useState(null);
  const [currentCommentPage, setCurrentCommentPage] = useState(1);
  const [totalCommentPages, setTotalCommentPages] = useState(0);

  const [currentView, setCurrentView] = useState('posts');

  // --- НОВОЕ СОСТОЯНИЕ ДЛЯ УПРАВЛЕНИЯ ВИДИМОСТЬЮ КОММЕНТАРИЕВ ---
  const [isCommentsSectionVisible, setIsCommentsSectionVisible] = useState(false); // По умолчанию скрыты

  const loadComments = async (postId, page = 1) => {
    if (!postId) return;
    setIsLoadingComments(true);
    setCommentsError(null);
    try {
      const data = await fetchCommentsAPI(postId, page);
      setComments(data.comments);
      setTotalCommentPages(Math.ceil(data.total_comments / COMMENTS_PER_PAGE));
      setCurrentCommentPage(page);
      // setIsCommentsSectionVisible(true); // Показываем секцию при успешной загрузке
    } catch (err) {
      setCommentsError(err.message);
      setComments([]);
      setTotalCommentPages(0);
      // setIsCommentsSectionVisible(false); // Скрываем при ошибке, или оставляем как есть
    } finally {
      setIsLoadingComments(false);
    }
  };

  const handleShowComments = (postId) => {
    if (selectedPostId === postId && isCommentsSectionVisible) {
      // Если кликнули на тот же пост и комментарии уже видны - скрыть их
      setIsCommentsSectionVisible(false);
      // Опционально: сбросить selectedPostId, чтобы кнопка "Комментарии" снова стала активной для этого поста
      // setSelectedPostId(null); 
      // setComments([]);
    } else {
      // Если выбран новый пост или комментарии были скрыты для текущего
      setSelectedPostId(postId);
      loadComments(postId, 1); // Загружаем первую страницу комментариев
      setIsCommentsSectionVisible(true); // Делаем секцию видимой
    }
  };
  
  // Функция для явного закрытия секции комментариев
  const handleCloseCommentsSection = () => {
    setIsCommentsSectionVisible(false);
    // Опционально: сбросить selectedPostId, чтобы не было "залипшего" состояния
    // setSelectedPostId(null);
    // setComments([]);
  };


  const handleCommentPageChange = (pageNumber) => {
    if (selectedPostId) {
      loadComments(selectedPostId, pageNumber);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>Инсайт-Компас</h1>
        <nav style={{ margin: '20px 0', display: 'flex', justifyContent: 'center', gap: '20px' }}>
          <button onClick={() => setCurrentView('posts')} disabled={currentView === 'posts'}>
            Просмотр постов
          </button>
          <button onClick={() => setCurrentView('dashboard')} disabled={currentView === 'dashboard'}>
            Дашборд
          </button>
          <button onClick={() => setCurrentView('channels')} disabled={currentView === 'channels'}>
            Управление каналами
          </button>
        </nav>
      </header>
      <main>
        {currentView === 'posts' && (
          <>
            <PostList onPostSelect={handleShowComments} selectedPostId={selectedPostId} isCommentsVisible={isCommentsSectionVisible} />
            {/* Рендерим CommentList только если есть выбранный пост И секция комментариев видима */}
            {selectedPostId && isCommentsSectionVisible && (
              <div className="comment-section-wrapper" style={{borderTop: '2px solid #ccc', marginTop: '30px', paddingTop: '10px'}}>
                <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px'}}>
                  <h3>Комментарии к посту ID: {selectedPostId}</h3>
                  <button onClick={handleCloseCommentsSection} style={{padding: '5px 10px', backgroundColor: '#f0f0f0'}}>
                    Закрыть комментарии
                  </button>
                </div>
                <CommentList
                  postId={selectedPostId} // postId все еще нужен для заголовка внутри CommentList, если он там есть
                  comments={comments}
                  isLoading={isLoadingComments}
                  error={commentsError}
                  currentPage={currentCommentPage}
                  totalPages={totalCommentPages}
                  onPageChange={handleCommentPageChange}
                />
              </div>
            )}
          </>
        )}
        {currentView === 'dashboard' && <DashboardPage />}
        {currentView === 'channels' && <ChannelManagementPage />}
      </main>
    </div>
  );
}

export default App;