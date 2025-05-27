// frontend/src/App.jsx

import React, { useState, useEffect } from 'react';
import './App.css';
import PostList from './components/PostList';
import CommentList from './components/CommentList';
// Импортируем константы
import { API_BASE_URL, COMMENTS_PER_PAGE } from './config'; // <--- ИЗМЕНЕНИЕ

function App() {
  const [selectedPostId, setSelectedPostId] = useState(null);
  const [comments, setComments] = useState([]);
  const [isLoadingComments, setIsLoadingComments] = useState(false);
  const [commentsError, setCommentsError] = useState(null);
  const [currentCommentPage, setCurrentCommentPage] = useState(1);
  const [totalCommentPages, setTotalCommentPages] = useState(0);

  const fetchComments = async (postId, page = 1) => {
    if (!postId) return;
    setIsLoadingComments(true);
    setCommentsError(null);
    try {
      const skip = (page - 1) * COMMENTS_PER_PAGE; // <--- Используем константу
      // Используем API_BASE_URL
      const response = await fetch(`${API_BASE_URL}/posts/${postId}/comments/?skip=${skip}&limit=${COMMENTS_PER_PAGE}`); 
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Ошибка HTTP: ${response.status}`);
      }
      const data = await response.json();
      setComments(data.comments);
      setTotalCommentPages(Math.ceil(data.total_comments / COMMENTS_PER_PAGE)); // <--- Используем константу
      setCurrentCommentPage(page);
    } catch (err) {
      console.error(`Ошибка при загрузке комментариев для поста ${postId}, страница ${page}:`, err);
      setCommentsError(err.message);
      setComments([]);
      setTotalCommentPages(0);
    } finally {
      setIsLoadingComments(false);
    }
  };

  // ... (handleShowComments, handleCommentPageChange и return без изменений) ...
  const handleShowComments = (postId) => {
    if (selectedPostId === postId) {
      return; 
    } else {
      setSelectedPostId(postId);
      fetchComments(postId, 1); 
    }
  };

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