// frontend/src/components/PostItem.jsx

import React from 'react';

// Простая функция для получения эмодзи по метке тональности
const getSentimentEmoji = (label) => {
  if (!label) return '❓'; // Если метки нет
  switch (label.toLowerCase()) {
    case 'positive':
      return '😊';
    case 'negative':
      return '😠';
    case 'neutral':
      return '😐';
    case 'mixed':
      return '🤔';
    default:
      return '❓';
  }
};

function PostItem({ post, onShowComments }) {
  if (!post) {
    return null;
  }

  const handleShowCommentsClick = () => {
    onShowComments(post.id);
  };

  return (
    <tr>
      <td>{post.channel.title} ({post.channel.username || `c/${post.channel.id}`})</td>
      <td>{post.post_text ? post.post_text.substring(0, 100) + '...' : '[Нет текста]'}</td>
      <td>{new Date(post.posted_at).toLocaleString()}</td>
      <td>{post.comments_count}</td>
      <td> {/* <--- НОВАЯ ЯЧЕЙКА ДЛЯ ТОНАЛЬНОСТИ */}
        {getSentimentEmoji(post.post_sentiment_label)} {post.post_sentiment_label ? post.post_sentiment_label.charAt(0).toUpperCase() + post.post_sentiment_label.slice(1) : 'N/A'}
      </td>
      <td>
        <button onClick={handleShowCommentsClick}>
          Комментарии
        </button>
      </td>
    </tr>
  );
}

export default PostItem;