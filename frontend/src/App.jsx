// frontend/src/App.jsx

import React, { useState, useEffect } from 'react';
import './App.css';
import PostList from './components/PostList';
import CommentList from './components/CommentList';
// Импортируем сервисную функцию (API_BASE_URL и COMMENTS_PER_PAGE из config уже не нужны здесь напрямую)
import { fetchCommentsAPI } from './services/apiService'; // <--- ИЗМЕНЕНИЕ
import { COMMENTS_PER_PAGE } from './config'; // COMMENTS_PER_PAGE все еще нужен для расчета totalCommentPages


function App() {
  const [selectedPostId, setSelectedPostId] = useState(null);
  const [comments, setComments] = useState([]);
  const [isLoadingComments, setIsLoadingComments] = useState(false);
  const [commentsError, setCommentsError] = useState(null);
  const [currentCommentPage, setCurrentCommentPage] = useState(1);
  const [totalCommentPages, setTotalCommentPages] = useState(0);

  // Функция для загрузки комментариев теперь использует сервис
  const loadComments = async (postId, page = 1) => { // Переименовали для ясности
    if (!postId) return;
    setIsLoadingComments(true);
    setCommentsError(null);
    try {
      // Используем сервисную функцию
      const data = await fetchCommentsAPI(postId, page); // <--- ИЗМЕНЕНИЕ
      setComments(data.comments);
      setTotalCommentPages(Math.ceil(data.total_comments / COMMENTS_PER_PAGE));
      setCurrentCommentPage(page);
    } catch (err) {
      // console.error уже будет в apiService
      setCommentsError(err.message);
      setComments([]);
      setTotalCommentPages(0);
    } finally {
      setIsLoadingComments(false);
    }
  };

  const handleShowComments = (postId) => {
    if (selectedPostId === postId) {
      // При повторном клике на тот же пост, можно перезагрузить первую страницу
      // или просто не делать ничего, если комментарии уже загружены
      // Для простоты, если хотим чтобы ничего не происходило:
      // if(comments.length > 0 && currentCommentPage === 1) return; 
      // loadComments(postId, 1);
      return; // Пока оставим так - ничего не делать при повторном клике на тот же самый
    } else {
      setSelectedPostId(postId);
      loadComments(postId, 1); 
    }
  };

  const handleCommentPageChange = (pageNumber) => {
    if (selectedPostId) {
      loadComments(selectedPostId, pageNumber);
    }
  };

  // ... (return без изменений) ...
  return (
    <div className="App">
      <header className="App-header">
        <h1>Инсайт-Компас - Собранные данные</h1>
      </header>
      <main>
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
      </main>
    </div>
  );
}

export default App;