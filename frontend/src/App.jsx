// frontend/src/App.jsx

import React, { useState, useEffect } from 'react'; // useEffect уже был, но убедимся
import './App.css';
import PostList from './components/PostList';
import CommentList from './components/CommentList';

const API_BASE_URL = 'http://localhost:8000/api/v1'; // Можно вынести в отдельный файл конфигурации позже

function App() {
  const [selectedPostId, setSelectedPostId] = useState(null);
  
  // Состояния для комментариев
  const [comments, setComments] = useState([]);
  const [isLoadingComments, setIsLoadingComments] = useState(false);
  const [commentsError, setCommentsError] = useState(null);

  // Функция для загрузки комментариев
  const fetchComments = async (postId) => {
    if (!postId) return; // Не загружаем, если нет postId

    setIsLoadingComments(true);
    setCommentsError(null);
    setComments([]); // Очищаем предыдущие комментарии
    try {
      // Пока без пагинации для комментариев, загружаем первые N (например, 100)
      const response = await fetch(`${API_BASE_URL}/posts/${postId}/comments/?skip=0&limit=100`); 
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Ошибка HTTP: ${response.status}`);
      }
      const data = await response.json(); // data будет { total_comments: X, comments: [...] }
      setComments(data.comments);
    } catch (err) {
      console.error(`Ошибка при загрузке комментариев для поста ${postId}:`, err);
      setCommentsError(err.message);
    } finally {
      setIsLoadingComments(false);
    }
  };

  // Обработчик выбора поста
  const handleShowComments = (postId) => {
    if (selectedPostId === postId) {
      // Если кликнули на тот же пост, скрываем комментарии (опционально)
      // setSelectedPostId(null);
      // setComments([]); 
      // Или можно просто не делать ничего, или перезагружать
      fetchComments(postId); // Перезагружаем, если кликнули на уже выбранный
    } else {
      setSelectedPostId(postId);
      fetchComments(postId); // Загружаем комментарии для нового поста
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
            isLoading={isLoadingComments} // Передаем isLoadingComments как isLoading
            error={commentsError}
          />
        )}
      </main>
    </div>
  );
}

export default App;