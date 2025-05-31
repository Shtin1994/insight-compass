// frontend/src/components/PostItem.jsx
import React from 'react';

const getSentimentEmoji = (label) => {
  if (!label) return '❓';
  switch (label.toLowerCase()) {
    case 'positive': return '😊';
    case 'negative': return '😠';
    case 'neutral':  return '😐';
    case 'mixed':    return '🤔';
    default:         return '❓';
  }
};

function PostItem({ post, onShowComments, onOpenPostModal }) {
  if (!post) {
    return null;
  }

  const handleShowCommentsClick = () => {
    onShowComments(post.id);
  };

  const handleOpenPostClick = () => {
    if (onOpenPostModal) {
      onOpenPostModal(post);
    }
  };

  return (
    <tr>
      <td>{post.channel.title} ({post.channel.username || `c/${post.channel.id}`})</td>
      {/* ИЗМЕНЕНИЕ ЗДЕСЬ: post.text_content вместо post.post_text */}
      <td>{post.text_content ? post.text_content.substring(0, 100) + (post.text_content.length > 100 ? '...' : '') : '[Нет текста]'}</td>
      <td>{new Date(post.posted_at).toLocaleString()}</td>
      <td>{post.comments_count}</td>
      <td>
        {getSentimentEmoji(post.post_sentiment_label)} {post.post_sentiment_label ? post.post_sentiment_label.charAt(0).toUpperCase() + post.post_sentiment_label.slice(1) : 'N/A'}
      </td>
      <td>
        <a href={post.link} target="_blank" rel="noopener noreferrer" title={post.link}>
          Ссылка
        </a>
      </td>
      <td>
        <button onClick={handleOpenPostClick}>
          Открыть пост
        </button>
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