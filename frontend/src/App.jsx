// frontend/src/App.jsx

import React, { useState, useEffect } from 'react';
import './App.css';
import PostList from './components/PostList';
import CommentList from './components/CommentList';
import DashboardPage from './components/DashboardPage'; // <--- Импортируем DashboardPage
import { fetchCommentsAPI } from './services/apiService';
import { COMMENTS_PER_PAGE } from './config';

function App() {
  const [selectedPostId, setSelectedPostId] = useState(null);
  const [comments, setComments] = useState([]);
  const [isLoadingComments, setIsLoadingComments] = useState(false);
  const [commentsError, setCommentsError] = useState(null);
  const [currentCommentPage, setCurrentCommentPage] = useState(1);
  const [totalCommentPages, setTotalCommentPages] = useState(0);

  // Состояние для переключения вида
  const [currentView, setCurrentView] = useState('posts'); // 'posts' или 'dashboard'

  const loadComments = async (postId, page = 1) => {
    // ... (код loadComments без изменений)
    if (!postId) return;
    setIsLoadingComments(true);
    setCommentsError(null);
    try {
      const data = await fetchCommentsAPI(postId, page); 
      setComments(data.comments);
      setTotalCommentPages(Math.ceil(data.total_comments / COMMENTS_PER_PAGE));
      setCurrentCommentPage(page);
    } catch (err) {
      setCommentsError(err.message);
      setComments([]);
      setTotalCommentPages(0);
    } finally {
      setIsLoadingComments(false);
    }
  };

  const handleShowComments = (postId) => {
    // Сбрасываем комментарии и страницу при выборе нового поста
    if (selectedPostId !== postId) {
        setSelectedPostId(postId);
        loadComments(postId, 1); // Загружаем первую страницу комментариев
    } else {
        // Если кликнули на тот же пост, можно либо скрыть комментарии, либо ничего не делать
        // Пока оставим так, чтобы комментарии оставались видимыми
        // Если нужно скрывать:
        // setSelectedPostId(null);
        // setComments([]);
    }
  };

  const handleCommentPageChange = (pageNumber) => {
    if (selectedPostId) {
      loadComments(selectedPostId, pageNumber);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>Инсайт-Компас</h1> {/* Немного изменим заголовок */}
        <nav style={{ margin: '20px 0', display: 'flex', justifyContent: 'center', gap: '20px' }}>
          <button onClick={() => setCurrentView('posts')} disabled={currentView === 'posts'}>
            Просмотр постов
          </button>
          <button onClick={() => setCurrentView('dashboard')} disabled={currentView === 'dashboard'}>
            Дашборд
          </button>
        </nav>
      </header>
      <main>
        {currentView === 'posts' && (
          <>
            <PostList onPostSelect={handleShowComments} /> 
            {selectedPostId && (
              <CommentList 
                postId={selectedPostId}
                comments={comments}
                isLoading={isLoadingComments}
                error={commentsError}
                currentPage={currentCommentPage}
                totalPages={totalCommentPages}
                onPageChange={handleCommentPageChange}
              />
            )}
          </>
        )}
        {currentView === 'dashboard' && <DashboardPage />}
      </main>
    </div>
  );
}

export default App;