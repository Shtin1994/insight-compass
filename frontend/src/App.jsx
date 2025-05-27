// frontend/src/App.jsx

import React, { useState, useEffect } from 'react';
import './App.css';
import PostList from './components/PostList';
import CommentList from './components/CommentList';

const API_BASE_URL = 'http://localhost:8000/api/v1';
const COMMENTS_PER_PAGE = 5; // <--- Количество комментариев на странице

function App() {
  const [selectedPostId, setSelectedPostId] = useState(null);
  
  const [comments, setComments] = useState([]);
  const [isLoadingComments, setIsLoadingComments] = useState(false);
  const [commentsError, setCommentsError] = useState(null);
  
  // Состояния для пагинации комментариев
  const [currentCommentPage, setCurrentCommentPage] = useState(1);
  const [totalCommentPages, setTotalCommentPages] = useState(0);

  // Функция для загрузки комментариев с учетом страницы
  const fetchComments = async (postId, page = 1) => {
    if (!postId) return;

    setIsLoadingComments(true);
    setCommentsError(null);
    // setComments([]); // Не очищаем сразу, чтобы избежать мерцания при смене страницы
    try {
      const skip = (page - 1) * COMMENTS_PER_PAGE;
      const response = await fetch(`${API_BASE_URL}/posts/${postId}/comments/?skip=${skip}&limit=${COMMENTS_PER_PAGE}`); 
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Ошибка HTTP: ${response.status}`);
      }
      const data = await response.json();
      setComments(data.comments);
      setTotalCommentPages(Math.ceil(data.total_comments / COMMENTS_PER_PAGE));
      setCurrentCommentPage(page); // Устанавливаем текущую страницу
    } catch (err) {
      console.error(`Ошибка при загрузке комментариев для поста ${postId}, страница ${page}:`, err);
      setCommentsError(err.message);
      setComments([]);
      setTotalCommentPages(0);
    } finally {
      setIsLoadingComments(false);
    }
  };

  // Обработчик выбора поста
  const handleShowComments = (postId) => {
    if (selectedPostId === postId) {
      // Если кликнули на тот же пост, возможно, просто переключим на первую страницу или ничего не будем делать
      // Для простоты, если уже выбран, можно сбросить или перезагрузить первую страницу
      // setSelectedPostId(null); // Это скроет комментарии
      // setComments([]);
      // fetchComments(postId, 1); // Перезагружаем первую страницу
      // Если не нужно ничего делать при повторном клике на тот же пост, можно оставить пустым или:
      return; 
    } else {
      setSelectedPostId(postId);
      fetchComments(postId, 1); // Загружаем первую страницу комментариев для нового поста
    }
  };

  // Обработчик смены страницы комментариев
  const handleCommentPageChange = (pageNumber) => {
    if (selectedPostId) {
      fetchComments(selectedPostId, pageNumber);
    }
  };

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
            // Props для пагинации комментариев
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