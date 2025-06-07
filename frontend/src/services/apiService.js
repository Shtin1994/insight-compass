// frontend/src/services/apiService.js

import { API_BASE_URL, POSTS_PER_PAGE, COMMENTS_PER_PAGE, DEFAULT_SORT_BY, DEFAULT_SORT_ORDER } from '../config';

/**
 * Базовая функция для выполнения fetch запросов и обработки ответа.
 * @param {string} url - URL для запроса.
 * @param {object} options - Опции для fetch (method, headers, body и т.д.).
 * @returns {Promise<object|null>} - Данные из JSON ответа или null для пустого ответа.
 * @throws {Error} - В случае ошибки сети или неуспешного HTTP статуса.
 */
const fetchData = async (url, options = {}) => {
  console.log(`[apiService] Fetching data from: ${url} with options:`, options); // Обновил лог
  try {
    const response = await fetch(url, options);
    if (!response.ok) {
      let errorDetail = `Ошибка HTTP: ${response.status} ${response.statusText}`;
      try {
        const errorData = await response.json();
        errorDetail = errorData.detail || errorData.message || errorDetail; 
        if (Array.isArray(errorData.detail)) { 
          errorDetail = errorData.detail.map(err => `${err.loc.join('.')} - ${err.msg}`).join('; ');
        }
      } catch (e) { 
        console.warn(`[apiService] Could not parse error response body as JSON for URL: ${url}`);
      }
      throw new Error(errorDetail);
    }
    const contentType = response.headers.get("content-type");
    if (response.status === 204 || !contentType || !contentType.includes("application/json")) {
        return null; 
    }
    return await response.json();
  } catch (error) {
    console.error(`[apiService] Ошибка fetch для URL ${url}:`, error.message);
    throw error; 
  }
};

/**
 * Загружает список постов с пагинацией, опциональным поисковым запросом и сортировкой.
 */
export const fetchPostsAPI = async (
  page = 1, 
  searchQuery = null, 
  sortBy = DEFAULT_SORT_BY,
  sortOrder = DEFAULT_SORT_ORDER
) => {
  const params = new URLSearchParams({
    page: page.toString(),
    limit: POSTS_PER_PAGE.toString(),
  });
  if (searchQuery && searchQuery.trim() !== '') {
    params.append('search_query', searchQuery.trim());
  }
  if (sortBy) {
    params.append('sort_by', sortBy);
  }
  if (sortOrder) {
    params.append('sort_order', sortOrder);
  }
  const url = `${API_BASE_URL}/posts/?${params.toString()}`;
  return fetchData(url);
};

/**
 * Загружает комментарии для указанного поста с пагинацией.
 */
export const fetchCommentsAPI = async (postId, page = 1) => {
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

// --- Функции для работы с каналами ---
export const fetchChannelsAPI = async (page = 1, limit = 10) => {
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

/**
 * Загружает агрегированные AI-инсайты из комментариев.
 */
export const fetchCommentInsightsAPI = async (daysPeriod = 7, topN = 10) => {
  const url = `${API_BASE_URL}/dashboard/comment_insights?days_period=${daysPeriod}&top_n=${topN}`;
  return fetchData(url);
};

/**
 * Загружает данные о тренде для конкретного AI-инсайта.
 */
export const fetchInsightItemTrendAPI = async (itemType, itemText, daysPeriod = 30, granularity = 'day') => {
  if (!itemType || !itemText) {
    console.error("[apiService] fetchInsightItemTrendAPI: itemType и itemText обязательны.");
    throw new Error("itemType и itemText обязательны для запроса тренда.");
  }
  const params = new URLSearchParams({
    item_type: itemType,
    item_text: itemText,
    days_period: daysPeriod.toString(),
    granularity: granularity,
  });
  const url = `${API_BASE_URL}/dashboard/insight_item_trend?${params.toString()}`;
  return fetchData(url);
};

/**
 * Отправляет запрос пользователя на естественном языке на бэкенд.
 * @param {string} queryText - Текст вопроса пользователя.
 * @param {object} [contextParams] - Опциональные параметры контекста (например, { days_period: 7 }).
 * @returns {Promise<object>} - Ответ от API, содержащий ответ AI.
 */
export const postNLQueryAPI = async (queryText, contextParams = {}) => {
  if (!queryText || queryText.trim() === '') {
    console.error("[apiService] postNLQueryAPI: queryText не может быть пустым.");
    throw new Error("Текст запроса не может быть пустым.");
  }
  
  const payload = {
    query_text: queryText,
    ...contextParams // Добавляем опциональные параметры контекста, если они есть
  };

  const url = `${API_BASE_URL}/natural_language_query/`; 
  return fetchData(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
};

// --- НАЧАЛО: НОВАЯ ФУНКЦИЯ ДЛЯ ПРОДВИНУТОГО ОБНОВЛЕНИЯ ДАННЫХ ---
/**
 * Запускает задачу продвинутого обновления данных на бэкенде.
 * @param {object} refreshParams - Объект с параметрами для обновления, 
 *                                 соответствующий схеме AdvancedDataRefreshRequest на бэкенде.
 * @returns {Promise<object>} - Ответ от API, обычно содержащий task_id.
 */
export const runAdvancedDataRefreshAPI = async (refreshParams) => {
  const url = `${API_BASE_URL}/run-advanced-data-refresh/`;
  return fetchData(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(refreshParams),
  });
};
// --- КОНЕЦ: НОВАЯ ФУНКЦИЯ ДЛЯ ПРОДВИНУТОГО ОБНОВЛЕНИЯ ДАННЫХ ---

// --- Функции для запуска других Celery задач (если они еще не были добавлены) ---
/**
 * Запускает задачу сбора данных.
 */
export const runCollectionTaskAPI = async () => {
  const url = `${API_BASE_URL}/run-collection-task/`;
  return fetchData(url, { method: 'POST' });
};

/**
 * Запускает задачу суммаризации постов.
 */
export const runSummarizationTaskAPI = async () => {
  const url = `${API_BASE_URL}/run-summarization-task/`;
  return fetchData(url, { method: 'POST' });
};

/**
 * Запускает задачу отправки дайджеста.
 */
export const runDailyDigestTaskAPI = async () => {
  const url = `${API_BASE_URL}/run-daily-digest-task/`;
  return fetchData(url, { method: 'POST' });
};

/**
 * Запускает задачу анализа тональности постов.
 */
export const runSentimentAnalysisTaskAPI = async () => {
  const url = `${API_BASE_URL}/run-sentiment-analysis-task/`;
  return fetchData(url, { method: 'POST' });
};

/**
 * Запускает задачу AI-анализа фич комментариев.
 * @param {number} limit - Количество комментариев для постановки в очередь.
 */
export const runCommentFeatureAnalysisAPI = async (limit = 100) => {
  const url = `${API_BASE_URL}/run-comment-feature-analysis/?limit=${limit}`;
  return fetchData(url, { method: 'POST' });
};