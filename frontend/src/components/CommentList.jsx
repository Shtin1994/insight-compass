// frontend/src/components/CommentList.jsx

import React from 'react';
import CommentItem from './CommentItem';

// mockComments больше не нужны, удаляем их

function CommentList({ postId, comments, isLoading, error }) { 
  // Принимаем comments, isLoading, error из props

  if (!postId) { // Это условие может быть уже не нужно, т.к. App.jsx его контролирует
    return null; 
  }

  if (isLoading) {
    return (
      <div className="comment-list" style={{ marginTop: '20px', borderTop: '1px solid #ccc', paddingTop: '10px' }}>
        <h3>Комментарии к посту ID: {postId}</h3>
        <p>Загрузка комментариев...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="comment-list" style={{ marginTop: '20px', borderTop: '1px solid #ccc', paddingTop: '10px' }}>
        <h3>Комментарии к посту ID: {postId}</h3>
        <p>Ошибка загрузки комментариев: {error}</p>
      </div>
    );
  }

  if (!comments || comments.length === 0) { // Проверяем comments из props
    return (
      <div className="comment-list" style={{ marginTop: '20px', borderTop: '1px solid #ccc', paddingTop: '10px' }}>
        <h3>Комментарии к посту ID: {postId}</h3>
        <p>Комментариев к этому посту пока нет.</p>
      </div>
    );
  }

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
          {comments.map((comment) => ( // Используем comments из props
            <CommentItem key={comment.id} comment={comment} />
          ))}
        </tbody>
      </table>
      {/* Пагинация для комментариев будет добавлена позже, если потребуется */}
    </div>
  );
}

export default CommentList;