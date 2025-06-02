// frontend/src/config.js

export const API_BASE_URL = 'http://localhost:8000/api/v1';
export const POSTS_PER_PAGE = 5;    // Используется в PostList.jsx
export const COMMENTS_PER_PAGE = 5; // Используется в App.jsx
export const SORT_BY_OPTIONS = [
  { value: 'posted_at', label: 'Дате публикации' },
  { value: 'comments_count', label: 'Количеству комментариев' },
  { value: 'views_count', label: 'Количеству просмотров' },
  { value: 'forwards_count', label: 'Количеству пересылок' },
  { value: 'reactions_total_sum', label: 'Общей сумме реакций' },
];

export const SORT_ORDER_OPTIONS = [
  { value: 'desc', label: 'По убыванию' },
  { value: 'asc', label: 'По возрастанию' },
];

export const DEFAULT_SORT_BY = 'posted_at';
export const DEFAULT_SORT_ORDER = 'desc';