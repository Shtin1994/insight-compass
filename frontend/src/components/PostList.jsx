// frontend/src/components/PostList.jsx

import React, { useState, useEffect } from 'react';
import PostItem from './PostItem';
import Pagination from './Pagination';
import { POSTS_PER_PAGE } from '../config';
import { fetchPostsAPI } from '../services/apiService';

function PostList({ onPostSelect }) { 
  const [posts, setPosts] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);

  useEffect(() => {
    const loadPosts = async () => { 
      setIsLoading(true);
      setError(null);
      try {
        const data = await fetchPostsAPI(currentPage); 
        setPosts(data.posts);
        setTotalPages(Math.ceil(data.total_posts / POSTS_PER_PAGE));
      } catch (err) {
        setError(err.message);
        setPosts([]);
        setTotalPages(0);
      } finally {
        setIsLoading(false);
      }
    };
    loadPosts();
  }, [currentPage]);

  const handlePageChange = (pageNumber) => {
    setCurrentPage(pageNumber);
  };

  const handleShowComments = (postId) => {
    if (onPostSelect) {
      onPostSelect(postId);
    }
  };

  if (isLoading) {
    return (
      <div className="spinner-container">
        <div className="spinner"></div>
      </div>
    );
  }

  if (error) {
    return <p>Ошибка загрузки постов: {error}</p>;
  }
  
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
            <th>Тональность</th> {/* <--- НОВЫЙ ЗАГОЛОВОК */}
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