// frontend/src/components/CommentItem.jsx

import React from 'react';

function CommentItem({ comment }) { // Принимаем объект comment
  if (!comment) {
    return null;
  }

  return (
    <tr>
      <td>{comment.author_display_name}</td>
      <td>{comment.text}</td>
      <td>{new Date(comment.commented_at).toLocaleString()}</td>
    </tr>
  );
}

export default CommentItem;