// frontend/src/services/apiService.js
// ЗАМЕНИТЕ СОДЕРЖИМОЕ ФАЙЛА НА ЭТО:

import { API_BASE_URL, POSTS_PER_PAGE, COMMENTS_PER_PAGE, DEFAULT_SORT_BY, DEFAULT_SORT_ORDER } from '../config'; // Добавлены DEFAULT_SORT_BY, DEFAULT_SORT_ORDER

/**
 * Базовая функция для выполнения fetch запросов и обработки ответа.
 * @param {string} url - URL для запроса.
 * @param {object} options - Опции для fetch (method, headers, body и т.д.).
 * @returns {Promise<object|null>} - Данные из JSON ответа или null для пустого ответа.
 * @throws {Error} - В случае ошибки сети или неуспешного HTTP статуса.
 */
const fetchData = async (url, options = {}) => {
  try {
    const response = await fetch(url, options);
    if (!response.ok) {
      let errorDetail = `Ошибка HTTP: ${response.status} ${response.statusText}`;
      try {
        const errorData = await response.json();
        errorDetail = errorData.detail || errorDetail;
        if (Array.isArray(errorData.detail)) {
          errorDetail = errorData.detail.map(err => `${err.loc.join('.')} - ${err.msg}`).join('; ');
        }
      } catch (e) { /* ignore */ }
      throw new Error(errorDetail);
    }
    const contentType = response.headers.get("content-type");
    if (response.status === 204 || !contentType || !contentType.includes("application/json")) { // Улучшена проверка contentType
        return null;
    }
    return await response.json();
  } catch (error) {
    console.error(`Ошибка fetch для URL ${url}:`, error.message);
    throw error;
  }
};

/**
 * Загружает список постов с пагинацией, опциональным поисковым запросом и сортировкой.
 * @param {number} page - Номер страницы.
 * @param {string} [searchQuery] - Опциональный поисковый запрос.
 * @param {string} [sortBy] - Поле для сортировки.
 * @param {string} [sortOrder] - Порядок сортировки ('asc' или 'desc').
 */
export const fetchPostsAPI = async (
  page = 1, 
  searchQuery = null, 
  sortBy = DEFAULT_SORT_BY, // Используем значения по умолчанию из config
  sortOrder = DEFAULT_SORT_ORDER // Используем значения по умолчанию из config
) => {
  // Согласно вашему отчету, бэкенд ожидает 'page' и 'limit'.
  let url = `${API_BASE_URL}/posts/?page=${page}&limit=${POSTS_PER_PAGE}`;

  if (searchQuery && searchQuery.trim() !== '') {
    url += `&search_query=${encodeURIComponent(searchQuery.trim())}`;
  }
  if (sortBy) {
    url += `&sort_by=${encodeURIComponent(sortBy)}`;
  }
  if (sortOrder) {
    url += `&sort_order=${encodeURIComponent(sortOrder)}`;
  }
  return fetchData(url);
};

/**
 * Загружает комментарии для указанного поста с пагинацией.
 */
export const fetchCommentsAPI = async (postId, page = 1) => {
  // Бэкенд для комментариев может ожидать skip/limit или page/limit.
  // Если он ожидает page/limit, то:
  // const url = `${API_BASE_URL}/posts/${postId}/comments/?page=${page}&limit=${COMMENTS_PER_PAGE}`;
  // Если он ожидает skip/limit, как в вашем исходном коде:
  const skip = (page - 1) * COMMENTS_PER_PAGE;
  const url = `${API_BASE_URL}/posts/${postId}/comments/?skip=${skip}&limit=${COMMENTS_PER_PAGE}`;
  return fetchData(url);
};

/**
 * Загружает общую статистику для дашборда.
 */
export const fetchDashboardStatsAPI = async () => {
  const url = `${API_BASE_URL}/dashboard/stats`;
  return fetchData(url);
};

/**
 * Загружает данные об активности по времени для дашборда.
 */
export const fetchActivityOverTimeAPI = async (days = 7) => {
  const url = `${API_BASE_URL}/dashboard/activity_over_time?days=${days}`;
  return fetchData(url);
};

/**
 * Загружает топ каналов по указанной метрике.
 */
export const fetchTopChannelsAPI = async (metric = 'posts', limit = 5, daysPeriod = 7) => {
  const url = `${API_BASE_URL}/dashboard/top_channels?metric=${metric}&limit=${limit}&days_period=${daysPeriod}`;
  return fetchData(url);
};

/**
 * Загружает данные о распределении тональности постов.
 */
export const fetchSentimentDistributionAPI = async (daysPeriod = 7) => {
  const url = `${API_BASE_URL}/dashboard/sentiment_distribution?days_period=${daysPeriod}`;
  return fetchData(url);
};

// --- Функции для работы с каналами (без изменений) ---
export const fetchChannelsAPI = async (page = 1, limit = 10) => {
  // Если бэкенд для каналов ожидает page/limit:
  // const url = `${API_BASE_URL}/channels/?page=${page}&limit=${limit}`;
  // Если он ожидает skip/limit, как в вашем исходном коде:
  const skip = (page - 1) * limit;
  const url = `${API_BASE_URL}/channels/?skip=${skip}&limit=${limit}`;
  return fetchData(url);
};

export const addChannelAPI = async (identifier) => {
  const url = `${API_BASE_URL}/channels/`;
  return fetchData(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ identifier: identifier }),
  });
};

export const updateChannelAPI = async (channelId, data) => {
  const url = `${API_BASE_URL}/channels/${channelId}/`;
  return fetchData(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
};

export const deleteChannelAPI = async (channelId) => {
  const url = `${API_BASE_URL}/channels/${channelId}/`;
  return fetchData(url, { method: 'DELETE' });
};

export const fetchChannelDetailsAPI = async (channelId) => {
  const url = `${API_BASE_URL}/channels/${channelId}/`;
  return fetchData(url);
};