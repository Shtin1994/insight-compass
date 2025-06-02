// frontend/src/components/PostItem.jsx

import React from 'react';

const getSentimentEmoji = (label) => {
  if (!label) return '❓';
  switch (label.toLowerCase()) {
    case 'positive': return '😊';
    case 'negative': return '😠';
    case 'neutral':  return '😐';
    case 'mixed':    return '🤔'; // Если у вас есть такая метка
    default:         return '❓';
  }
};

// Функция для форматирования даты
const formatDate = (dateString) => {
  if (!dateString) return 'N/A';
  try {
    // Используем более короткий формат для таблицы
    return new Date(dateString).toLocaleDateString('ru-RU', {
      day: '2-digit',
      month: '2-digit',
      year: '2-digit', // Используем 2-значный год для экономии места
    });
  } catch (e) {
    console.error("Error formatting date:", dateString, e);
    return dateString; // Возвращаем как есть, если не удалось распарсить
  }
};

// Функция для получения суммы реакций
const getTotalReactions = (reactionsData) => {
  // Если reactions_total_sum уже есть, используем его
  if (typeof reactionsData.reactions_total_sum === 'number') {
    return reactionsData.reactions_total_sum;
  }
  // Иначе считаем из объекта reactions
  if (reactionsData.reactions && typeof reactionsData.reactions === 'object') {
    return Object.values(reactionsData.reactions).reduce((sum, count) => sum + (Number(count) || 0), 0);
  }
  return 0; // Если нет ни того, ни другого
};


function PostItem({ post, onShowComments, onOpenPostModal }) {
  if (!post) {
    return null; // Лучше не рендерить ничего, если нет поста
  }

  const handleShowCommentsClick = () => {
    if (onShowComments) { // Проверка, что onShowComments передана
        onShowComments(post.id);
    }
  };

  const handleOpenPostClick = () => {
    if (onOpenPostModal) {
      onOpenPostModal(post);
    }
  };

  const displayText = post.text_content || post.caption_text || post.summary_text || "[Нет текста]";
  const previewText = displayText.substring(0, 70) + (displayText.length > 70 ? "..." : ""); // Уменьшил превью для таблицы

  const reactionsSum = getTotalReactions(post); // post может содержать post.reactions_total_sum или post.reactions

  // Формируем title для ячейки с реакциями, чтобы показать детали при наведении
  let reactionsTitle = '';
  if (post.reactions && typeof post.reactions === 'object' && Object.keys(post.reactions).length > 0) {
    reactionsTitle = Object.entries(post.reactions)
      .map(([emoji, count]) => `${emoji}: ${count}`)
      .join(', ');
  } else if (typeof post.reactions_total_sum === 'number') {
    reactionsTitle = `Всего: ${post.reactions_total_sum}`;
  }


  return (
    <tr className="post-item-row hover:bg-gray-50"> {/* Пример класса для стилизации */}
      <td style={{ padding: '8px', border: '1px solid #ddd', verticalAlign: 'top' }}>
        {post.channel?.title || 'N/A'}
        {post.channel?.username && <div style={{fontSize: '0.8em', color: '#555'}}>@{post.channel.username}</div>}
      </td>
      <td style={{ padding: '8px', border: '1px solid #ddd', verticalAlign: 'top', wordBreak: 'break-word' }} title={displayText}>
        {previewText}
      </td>
      <td style={{ padding: '8px', border: '1px solid #ddd', verticalAlign: 'top', whiteSpace: 'nowrap' }}>
        {formatDate(post.posted_at)}
      </td>
      <td style={{ padding: '8px', border: '1px solid #ddd', textAlign: 'center', verticalAlign: 'top' }}>
        {post.comments_count ?? 0} {/* Используем ?? 0 для отображения 0 вместо N/A */}
      </td>
      {/* --- НАЧАЛО ИЗМЕНЕНИЙ: Отображение новых данных --- */}
      <td style={{ padding: '8px', border: '1px solid #ddd', textAlign: 'center', verticalAlign: 'top' }}>
        {post.views_count ?? 0}
      </td>
      <td style={{ padding: '8px', border: '1px solid #ddd', textAlign: 'center', verticalAlign: 'top' }}>
        {post.forwards_count ?? 0}
      </td>
      <td style={{ padding: '8px', border: '1px solid #ddd', textAlign: 'center', verticalAlign: 'top' }} title={reactionsTitle}>
        {reactionsSum}
      </td>
      {/* --- КОНЕЦ ИЗМЕНЕНИЙ --- */}
      <td style={{ padding: '8px', border: '1px solid #ddd', verticalAlign: 'top' }}>
        {getSentimentEmoji(post.post_sentiment_label)} 
        <span style={{marginLeft: '4px'}}>{post.post_sentiment_label ? (post.post_sentiment_label.charAt(0).toUpperCase() + post.post_sentiment_label.slice(1)) : 'N/A'}</span>
        {/* {post.post_sentiment_score ? ` (${post.post_sentiment_score.toFixed(2)})` : ''} // Можно убрать score для экономии места */}
      </td>
      <td style={{ padding: '8px', border: '1px solid #ddd', textAlign: 'center', verticalAlign: 'top' }}>
        <a href={post.link} target="_blank" rel="noopener noreferrer" title={post.link} style={{textDecoration: 'none'}}>
          🔗
        </a>
      </td>
      <td style={{ padding: '8px', border: '1px solid #ddd', textAlign: 'center', verticalAlign: 'top' }}>
        <button onClick={handleOpenPostClick} style={{ padding: '5px 8px', fontSize: '0.9em' }}>
          Открыть
        </button>
      </td>
      {/* Я убрал кнопку "Комментарии" из PostItem, так как она дублирует функционал столбца "Комм." и не ясно, как должна работать без отображения комментариев прямо здесь */}
      {/* Если она нужна, ее можно вернуть, но нужно продумать ее взаимодействие */}
    </tr>
  );
}

export default PostItem;