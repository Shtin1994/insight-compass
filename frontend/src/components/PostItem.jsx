// frontend/src/components/PostItem.jsx

import React from 'react';

function PostItem({ post, onShowComments }) { // Принимаем объект post и функцию onShowComments
  if (!post) {
    return null; // На случай, если post не передан
  }

  const handleShowCommentsClick = () => {
    onShowComments(post.id); // Вызываем переданную функцию с ID поста
  };

  return (
    <tr>
      <td>{post.channel.title} ({post.channel.username || `c/${post.channel.id}`})</td>
      <td>{post.post_text ? post.post_text.substring(0, 100) + '...' : '[Нет текста]'}</td>
      <td>{new Date(post.posted_at).toLocaleString()}</td>
      <td>{post.comments_count}</td>
      <td>
        <button onClick={handleShowCommentsClick}>
          Комментарии
        </button>
      </td>
    </tr>
  );
}

export default PostItem;