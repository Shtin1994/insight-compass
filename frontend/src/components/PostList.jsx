// frontend/src/components/PostList.jsx

import React, { useState, useEffect } from 'react';
import PostItem from './PostItem';
import Pagination from './Pagination';
import { POSTS_PER_PAGE } from '../config'; // API_BASE_URL больше не нужен здесь напрямую
// Импортируем сервисную функцию
import { fetchPostsAPI } from '../services/apiService'; // <--- ИЗМЕНЕНИЕ

function PostList({ onPostSelect }) { 
  const [posts, setPosts] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);

  useEffect(() => {
    const loadPosts = async () => { // Переименовали для ясности
      setIsLoading(true);
      setError(null);
      try {
        // Используем сервисную функцию
        const data = await fetchPostsAPI(currentPage); // <--- ИЗМЕНЕНИЕ
        setPosts(data.posts);
        setTotalPages(Math.ceil(data.total_posts / POSTS_PER_PAGE));
      } catch (err) {
        // console.error уже будет в apiService, здесь только устанавливаем ошибку для UI
        setError(err.message);
        setPosts([]);
        setTotalPages(0);
      } finally {
        setIsLoading(false);
      }
    };
    loadPosts();
  }, [currentPage]);

  // ... (handlePageChange, handleShowComments и return без изменений) ...
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
  
  // Для totalPostsFromState лучше использовать totalPages, если он уже есть
  const noPostsAvailable = posts.length === 0 && totalPages === 0 && !isLoading;

  if (noPostsAvailable) { 
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