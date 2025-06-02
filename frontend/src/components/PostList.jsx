// frontend/src/components/PostList.jsx

import React, { useState, useEffect, useCallback } from 'react';
import PostItem from './PostItem';
import Pagination from './Pagination';
import Modal from './Modal';
import { POSTS_PER_PAGE, SORT_BY_OPTIONS, SORT_ORDER_OPTIONS, DEFAULT_SORT_BY, DEFAULT_SORT_ORDER } from '../config';
import { fetchPostsAPI } from '../services/apiService';

function PostList({ onPostSelect, selectedPostId, isCommentsVisible }) {
  const [posts, setPosts] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);

  const [searchQueryInput, setSearchQueryInput] = useState('');
  const [activeSearchQuery, setActiveSearchQuery] = useState('');

  const [sortBy, setSortBy] = useState(DEFAULT_SORT_BY);
  const [sortOrder, setSortOrder] = useState(DEFAULT_SORT_ORDER);

  const [isPostModalOpen, setIsPostModalOpen] = useState(false);
  const [selectedPostContent, setSelectedPostContent] = useState({ title: '', text: '' });

  const loadPosts = useCallback(async (page, query, currentSortBy, currentSortOrder) => {
    setIsLoading(true);
    setError(null);
    console.log(`[PostList] Loading posts - Page: ${page}, Query: '${query}', SortBy: ${currentSortBy}, SortOrder: ${currentSortOrder}, Limit: ${POSTS_PER_PAGE}`);
    try {
      const data = await fetchPostsAPI(page, query, currentSortBy, currentSortOrder);
      console.log("[PostList] Data from API (raw object):", data); 
      
      // --- НАЧАЛО ИЗМЕНЕНИЯ: Новый лог для проверки ID из data.posts ---
      if (data && data.posts && Array.isArray(data.posts)) {
        console.log("[PostList] IDs from data.posts received from API:", data.posts.map(p => p.id));
      } else {
        console.warn("[PostList] data.posts is not an array or is missing in API response:", data ? data.posts : 'data is null/undefined');
      }
      // --- КОНЕЦ ИЗМЕНЕНИЯ ---

      const newPosts = data && data.posts && Array.isArray(data.posts) ? [...data.posts] : [];
      setPosts(newPosts); 
      console.log("[PostList] Set new posts to state (IDs):", newPosts.map(p => p.id));

      const totalItemsFromApi = data ? data.total_posts : 0; 
      console.log("[PostList] Total items from API (data.total_posts):", totalItemsFromApi);

      const calculatedTotalPages = totalItemsFromApi ? Math.ceil(totalItemsFromApi / POSTS_PER_PAGE) : 0;
      setTotalPages(calculatedTotalPages);
      console.log(`[PostList] Calculated Total Pages: ${calculatedTotalPages}`);
      
    } catch (err) {
      console.error("[PostList] Error in loadPosts:", err);
      setError(err.message || 'Не удалось загрузить посты');
      setPosts([]);
      setTotalPages(0);
    } finally {
      setIsLoading(false);
    }
  }, [POSTS_PER_PAGE]);

  useEffect(() => {
    loadPosts(currentPage, activeSearchQuery, sortBy, sortOrder);
  }, [currentPage, activeSearchQuery, sortBy, sortOrder, loadPosts]);

  const handlePageChange = (pageNumber) => {
    console.log(`[PostList] handlePageChange called with: ${pageNumber}`);
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

  const handleSortByChange = (event) => {
    if (currentPage !== 1) setCurrentPage(1);
    setSortBy(event.target.value);
  };

  const handleSortOrderChange = (event) => {
    if (currentPage !== 1) setCurrentPage(1);
    setSortOrder(event.target.value);
  };

  const handleOpenPostModal = (post) => {
    const fullText = [post.text_content, post.caption_text].filter(Boolean).join("\n\n---\nCaption:\n");
    setSelectedPostContent({
        title: `Пост из канала "${post.channel?.title || 'Неизвестный канал'}" (ID поста: ${post.id})`,
        text: fullText || post.summary_text || "[Нет текстового содержимого или резюме]"
    });
    setIsPostModalOpen(true);
  };

  const handleClosePostModal = () => {
    setIsPostModalOpen(false);
    setSelectedPostContent({ title: '', text: '' });
  };
  
  console.log(`[PostList] Rendering - CurrentPage: ${currentPage}, TotalPages: ${totalPages}, Posts count: ${posts.length}, Posts IDs: ${posts.map(p => p.id).join(', ')}`);

  if (isLoading && posts.length === 0) {
    return (
      <div className="spinner-container" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '200px' }}>
        <div className="spinner"></div>
        <p style={{ marginLeft: '10px' }}>Загрузка постов...</p>
      </div>
    );
  }
  
  if (error) {
    return <p style={{ color: 'red', textAlign: 'center' }}>Ошибка загрузки постов: {error}</p>;
  }
  
  const noPostsAvailable = posts.length === 0 && !isLoading;

  return (
    <div className="post-list" style={{ padding: '20px' }}>
      <div className="controls-panel" style={{ display: 'flex', flexDirection: 'column', gap: '15px', marginBottom: '20px', padding: '15px', border: '1px solid #eee', borderRadius: '8px', background: '#f9f9f9' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '10px' }}>
          <h2 style={{ margin: 0 }}>Список постов</h2>
          <form onSubmit={handleSearchSubmit} style={{ display: 'flex', gap: '10px', flexGrow: 1, minWidth: '300px' }}>
            <input
              type="text"
              placeholder="Поиск по тексту, подписи, резюме..."
              value={searchQueryInput}
              onChange={handleSearchInputChange}
              style={{ padding: '8px', flexGrow: 1, border: '1px solid #ccc', borderRadius: '4px' }}
            />
            <button type="submit" style={{ padding: '8px 15px', background: '#007bff', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>Искать</button>
            {activeSearchQuery && (
              <button type="button" onClick={handleClearSearch} style={{ padding: '8px 10px', background: '#6c757d', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
                Сбросить
              </button>
            )}
          </form>
        </div>
        <div className="sort-controls" style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
          <label htmlFor="sort-by" style={{ fontWeight: 'bold' }}>Сортировать по:</label>
          <select
            id="sort-by"
            value={sortBy}
            onChange={handleSortByChange}
            style={{ padding: '8px', border: '1px solid #ccc', borderRadius: '4px' }}
          >
            {SORT_BY_OPTIONS.map(option => (
              <option key={option.value} value={option.value}>{option.label}</option>
            ))}
          </select>

          <label htmlFor="sort-order" style={{ fontWeight: 'bold', marginLeft: '15px' }}>Порядок:</label>
          <select
            id="sort-order"
            value={sortOrder}
            onChange={handleSortOrderChange}
            style={{ padding: '8px', border: '1px solid #ccc', borderRadius: '4px' }}
          >
            {SORT_ORDER_OPTIONS.map(option => (
              <option key={option.value} value={option.value}>{option.label}</option>
            ))}
          </select>
        </div>
      </div>

      {isLoading && posts.length > 0 && (
        <div style={{ textAlign: 'center', padding: '10px', color: '#555' }}>
          <i>Обновление списка...</i>
        </div>
      )}

      {activeSearchQuery && posts.length > 0 && !isLoading && (
        <p style={{ fontStyle: 'italic', marginBottom: '10px' }}>
          Результаты по запросу: "{activeSearchQuery}"
        </p>
      )}
      {activeSearchQuery && posts.length === 0 && !isLoading && (
        <p>По запросу "{activeSearchQuery}" ничего не найдено.</p>
      )}
      {noPostsAvailable && !activeSearchQuery && (
        <p style={{ textAlign: 'center', padding: '20px', color: '#666' }}>Постов пока нет. Попробуйте добавить каналы или изменить критерии поиска/фильтрации.</p>
      )}

      {posts.length > 0 && ! (isLoading && posts.length === 0) && ( // Условие, чтобы таблица не исчезала при isLoading=true и posts.length > 0
        <>
          <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' }}>
            <thead>
              <tr style={{ background: '#f0f0f0' }}>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left', width: '15%' }}>Канал</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left', width: '25%' }}>Текст (превью)</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left', width: '10%' }}>Дата</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'center', width: '7%' }}>Комм.</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'center', width: '7%' }}>Просм.</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'center', width: '7%' }}>Перес.</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'center', width: '7%' }}>Реакц.</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left', width: '10%' }}>Тональность</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'center', width: '5%' }}>Ссылка</th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'center', width: '7%' }}>Детали</th>
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
        <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', maxHeight: '70vh', overflowY: 'auto' }}>
            {selectedPostContent.text}
        </pre>
      </Modal>
    </div>
  );
}

export default PostList;