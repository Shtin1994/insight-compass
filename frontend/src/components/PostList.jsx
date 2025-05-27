// frontend/src/components/PostList.jsx

import React, { useState, useEffect } from 'react'; // <--- Добавляем useEffect
import PostItem from './PostItem';
import Pagination from './Pagination';

// Базовый URL нашего API (предполагается, что FastAPI запущен на порту 8000)
// ВАЖНО: Убедитесь, что ваш FastAPI сервер доступен по этому адресу из браузера.
// Если React и FastAPI на одном хосте, но разных портах, CORS может стать проблемой.
// Мы решим это позже, если возникнет.
const API_BASE_URL = 'http://localhost:8000/api/v1';

function PostList({ onPostSelect }) { 
  const [posts, setPosts] = useState([]); // <--- Состояние для хранения постов с API
  const [isLoading, setIsLoading] = useState(true); // <--- Состояние для индикатора загрузки
  const [error, setError] = useState(null); // <--- Состояние для ошибок загрузки

  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0); // <--- Будет приходить от API
  const postsPerPage = 5; // <--- Количество постов на странице, которое мы запрашиваем у API

  // useEffect для загрузки постов при монтировании компонента или изменении currentPage
  useEffect(() => {
    const fetchPosts = async () => {
      setIsLoading(true);
      setError(null);
      try {
        // Рассчитываем параметр skip для API
        const skip = (currentPage - 1) * postsPerPage;
        const response = await fetch(`${API_BASE_URL}/posts/?skip=${skip}&limit=${postsPerPage}`);
        
        if (!response.ok) {
          // Если ответ не 2xx, выбрасываем ошибку
          const errorData = await response.json();
          throw new Error(errorData.detail || `Ошибка HTTP: ${response.status}`);
        }
        
        const data = await response.json(); // data будет { total_posts: X, posts: [...] }
        setPosts(data.posts);
        setTotalPages(Math.ceil(data.total_posts / postsPerPage));
      } catch (err) {
        console.error("Ошибка при загрузке постов:", err);
        setError(err.message);
        setPosts([]); // Очищаем посты в случае ошибки
        setTotalPages(0);
      } finally {
        setIsLoading(false);
      }
    };

    fetchPosts();
  }, [currentPage, postsPerPage]); // <--- Зависимости: перезагружаем при смене страницы или лимита

  const handlePageChange = (pageNumber) => {
    setCurrentPage(pageNumber);
  };

  const handleShowComments = (postId) => {
    if (onPostSelect) {
      onPostSelect(postId);
    }
  };

  if (isLoading) {
    return <p>Загрузка постов...</p>;
  }

  if (error) {
    return <p>Ошибка загрузки постов: {error}</p>;
  }
  
  if (posts.length === 0 && totalPages === 0 && !isLoading) { // Проверка на полное отсутствие постов
    return <p>Постов пока нет.</p>;
  }
  
  return (
    <div className="post-list">
      <h2>Список постов</h2>
      <table>
        <thead>
          <tr>
            <th>Канал</th>
            <th>Текст поста (превью)</th>
            <th>Дата</th>
            <th>Комментарии</th>
            <th>Действие</th>
          </tr>
        </thead>
        <tbody>
          {posts.map((post) => ( 
            <PostItem 
              key={post.id} 
              post={post} 
              onShowComments={handleShowComments}
            />
          ))}
        </tbody>
      </table>
      <Pagination 
        currentPage={currentPage}
        totalPages={totalPages}
        onPageChange={handlePageChange}
      />
    </div>
  );
}

export default PostList;