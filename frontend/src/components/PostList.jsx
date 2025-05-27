// frontend/src/components/PostList.jsx

import React, { useState, useEffect } from 'react';
import PostItem from './PostItem';
import Pagination from './Pagination';
// Импортируем константы
import { API_BASE_URL, POSTS_PER_PAGE } from '../config'; // <--- ИЗМЕНЕНИЕ (путь к config.js)

function PostList({ onPostSelect }) { 
  const [posts, setPosts] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  // const postsPerPage = POSTS_PER_PAGE; // <--- Используем константу (можно прямо в useEffect)

  useEffect(() => {
    const fetchPosts = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const skip = (currentPage - 1) * POSTS_PER_PAGE; // <--- Используем константу
        // Используем API_BASE_URL
        const response = await fetch(`${API_BASE_URL}/posts/?skip=${skip}&limit=${POSTS_PER_PAGE}`);
        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.detail || `Ошибка HTTP: ${response.status}`);
        }
        const data = await response.json();
        setPosts(data.posts);
        setTotalPages(Math.ceil(data.total_posts / POSTS_PER_PAGE)); // <--- Используем константу
      } catch (err) {
        console.error("Ошибка при загрузке постов:", err);
        setError(err.message);
        setPosts([]);
        setTotalPages(0);
      } finally {
        setIsLoading(false);
      }
    };
    fetchPosts();
  }, [currentPage]); // <--- Убираем postsPerPage из зависимостей, т.к. он теперь константа

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
  
  const totalPostsFromState = posts.length > 0 ? totalPages * POSTS_PER_PAGE : 0; // Приблизительно, лучше total_posts из API

  if (posts.length === 0 && totalPostsFromState === 0 && !isLoading ) { 
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