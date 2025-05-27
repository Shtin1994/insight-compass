// frontend/src/components/CommentList.jsx

import React from 'react';
import CommentItem from './CommentItem';

// Обновленные mock-данные с postId
const allMockComments = [
  { postId: 1, id: 101, author_display_name: 'User123 (для поста 1)', text: 'Отличный пост 1! Полностью согласен.', commented_at: '2025-05-27T10:05:00Z' },
  { postId: 1, id: 102, author_display_name: 'HelperBot (для поста 1)', text: 'Спасибо за ваше мнение о посте 1.', commented_at: '2025-05-27T10:10:00Z' },
  
  { postId: 2, id: 201, author_display_name: 'TechGuru (для поста 2)', text: 'Интересные новости для поста 2!', commented_at: '2025-05-27T11:35:00Z' },
  
  { postId: 3, id: 301, author_display_name: 'Philosopher (для поста 3)', text: 'Глубокие мысли о посте 3.', commented_at: '2025-05-27T12:20:00Z' },
  { postId: 3, id: 302, author_display_name: 'Skeptic (для поста 3)', text: 'А я не уверен насчет выводов в посте 3.', commented_at: '2025-05-27T12:25:00Z' },
  { postId: 3, id: 303, author_display_name: 'Optimist (для поста 3)', text: 'Но в целом пост 3 заставляет задуматься!', commented_at: '2025-05-27T12:30:00Z' },
];

function CommentList({ postId }) {
  if (!postId) {
    return null; 
  }

  // Фильтруем комментарии по postId
  const commentsForThisPost = allMockComments.filter(comment => comment.postId === postId);

  if (!commentsForThisPost.length) {
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
          {commentsForThisPost.map((comment) => (
            <CommentItem key={comment.id} comment={comment} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default CommentList;