// frontend/src/components/PostList.jsx

import React, { useState, useEffect, useCallback } from 'react';
import PostItem from './PostItem';
import Pagination from './Pagination';
import Modal from './Modal';
import { POSTS_PER_PAGE } from '../config';
import { fetchPostsAPI } from '../services/apiService';

function PostList({ onPostSelect, selectedPostId, isCommentsVisible }) {
  const [posts, setPosts] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);

  const [searchQueryInput, setSearchQueryInput] = useState('');
  const [activeSearchQuery, setActiveSearchQuery] = useState('');

  const [isPostModalOpen, setIsPostModalOpen] = useState(false);
  const [selectedPostContent, setSelectedPostContent] = useState({ title: '', text: '' });


  const loadPosts = useCallback(async (page, query) => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await fetchPostsAPI(page, query);
      setPosts(data.posts);
      setTotalPages(Math.ceil(data.total_posts / POSTS_PER_PAGE));
    } catch (err) {
      setError(err.message);
      setPosts([]);
      setTotalPages(0);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadPosts(currentPage, activeSearchQuery);
  }, [currentPage, activeSearchQuery, loadPosts]);

  const handlePageChange = (pageNumber) => {
    setCurrentPage(pageNumber);
  };

  const handleShowComments = (postId) => {
    if (onPostSelect) {
      onPostSelect(postId);
    }
  };

  const handleSearchInputChange = (e) => {
    setSearchQueryInput(e.target.value);
  };

  const handleSearchSubmit = (e) => {
    e.preventDefault();
    if (currentPage !== 1) setCurrentPage(1);
    setActiveSearchQuery(searchQueryInput.trim());
  };

  const handleClearSearch = () => {
    setSearchQueryInput('');
    if (currentPage !== 1) setCurrentPage(1);
    setActiveSearchQuery('');
  };

  const handleOpenPostModal = (post) => {
    setSelectedPostContent({
        title: `Пост из канала "${post.channel.title}" (ID: ${post.id})`,
        // ИЗМЕНЕНИЕ ЗДЕСЬ: post.text_content вместо post.post_text
        text: post.text_content || "[Нет текста]"
    });
    setIsPostModalOpen(true);
  };

  const handleClosePostModal = () => {
    setIsPostModalOpen(false);
    setSelectedPostContent({ title: '', text: '' });
  };


  if (isLoading && posts.length === 0) {
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

  return (
    <div className="post-list">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
        <h2>Список постов</h2>
        <form onSubmit={handleSearchSubmit} style={{ display: 'flex', gap: '10px' }}>
          <input
            type="text"
            placeholder="Поиск по тексту или резюме..."
            value={searchQueryInput}
            onChange={handleSearchInputChange}
            style={{ padding: '8px', minWidth: '250px' }}
          />
          <button type="submit" style={{ padding: '8px 15px' }}>Искать</button>
          {activeSearchQuery && (
            <button type="button" onClick={handleClearSearch} style={{ padding: '8px 10px', backgroundColor: '#f0f0f0' }}>
              Сбросить
            </button>
          )}
        </form>
      </div>

      {activeSearchQuery && posts.length > 0 && (
        <p style={{ fontStyle: 'italic', marginBottom: '10px' }}>
          Результаты по запросу: "{activeSearchQuery}" (Найдено ~{totalPages * POSTS_PER_PAGE < posts.length ? totalPages * POSTS_PER_PAGE : posts.length } из { (totalPages * POSTS_PER_PAGE) })
        </p>
      )}
       {activeSearchQuery && posts.length === 0 && !isLoading && (
        <p>По запросу "{activeSearchQuery}" ничего не найдено.</p>
      )}

      {noPostsAvailable && !activeSearchQuery && (
        <p>Постов пока нет.</p>
      )}

      {posts.length > 0 && (
        <>
          <table>
            <thead>
              <tr>
                <th>Канал</th>
                <th>Текст поста (превью)</th>
                <th>Дата</th>
                <th>Комментарии</th>
                <th>Тональность</th>
                <th>Ссылка</th>
                <th>Просмотр</th>
                <th>Действие</th>
              </tr>
            </thead>
            <tbody>
              {posts.map((post) => (
                <PostItem
                  key={post.id}
                  post={post}
                  onShowComments={handleShowComments}
                  onOpenPostModal={handleOpenPostModal}
                />
              ))}
            </tbody>
          </table>
          <Pagination
            currentPage={currentPage}
            totalPages={totalPages}
            onPageChange={handlePageChange}
          />
        </>
      )}

      <Modal
        isOpen={isPostModalOpen}
        onClose={handleClosePostModal}
        title={selectedPostContent.title}
      >
        <pre>{selectedPostContent.text}</pre>
      </Modal>
    </div>
  );
}

export default PostList;