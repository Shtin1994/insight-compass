// frontend/src/components/CommentList.jsx

// ... (импорты как были) ...
import React from 'react';
import CommentItem from './CommentItem';
import Pagination from './Pagination';

function CommentList({ postId, comments, isLoading, error, currentPage, totalPages, onPageChange }) { 
  if (!postId && !isLoading) { 
    return null; 
  }

  if (isLoading) {
    return (
      <div className="comment-list" style={{ marginTop: '20px', borderTop: '1px solid #ccc', paddingTop: '10px' }}>
        {postId && <h3>Комментарии к посту ID: {postId}</h3>}
        {/* Используем спиннер */}
        <div className="spinner-container">
          <div className="spinner"></div>
        </div>
      </div>
    );
  }

  if (error) {
    // ... (обработка ошибки как была) ...
    return (
      <div className="comment-list" style={{ marginTop: '20px', borderTop: '1px solid #ccc', paddingTop: '10px' }}>
        {postId && <h3>Комментарии к посту ID: {postId}</h3>}
        <p>Ошибка загрузки комментариев: {error}</p>
      </div>
    );
  }

  if (!comments || comments.length === 0) {
    // ... (обработка отсутствия комментариев как была) ...
    return (
      <div className="comment-list" style={{ marginTop: '20px', borderTop: '1px solid #ccc', paddingTop: '10px' }}>
        {postId && <h3>Комментарии к посту ID: {postId}</h3>}
        <p>Комментариев к этому посту пока нет.</p>
      </div>
    );
  }

  // ... (return с таблицей комментариев и пагинацией как был) ...
  return (
    <div className="comment-list" style={{ marginTop: '20px', borderTop: '1px solid #ccc', paddingTop: '10px' }}>
      <h3>Комментарии к посту ID: {postId}</h3>
      <table>
        <thead>
          <tr>
            <th>Автор</th>
            <th>Текст комментария</th>
            <th>Дата</th>
          </tr>
        </thead>
        <tbody>
          {comments.map((comment) => (
            <CommentItem key={comment.id} comment={comment} />
          ))}
        </tbody>
      </table>
      <Pagination
        currentPage={currentPage}
        totalPages={totalPages}
        onPageChange={onPageChange}
      />
    </div>
  );
}

export default CommentList;