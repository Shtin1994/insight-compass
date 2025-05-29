// frontend/src/services/apiService.js

import { API_BASE_URL, POSTS_PER_PAGE, COMMENTS_PER_PAGE } from '../config';

/**
 * Базовая функция для выполнения fetch запросов и обработки ответа.
 * @param {string} url - URL для запроса.
 * @param {object} options - Опции для fetch (method, headers, body и т.д.).
 * @returns {Promise<object>} - Данные из JSON ответа.
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
      } catch (e) { /* ignore */ }
      throw new Error(errorDetail);
    }
    const contentType = response.headers.get("content-type");
    if (contentType && contentType.indexOf("application/json") !== -1) {
        return await response.json();
    } else {
        return null; 
    }
  } catch (error) {
    console.error(`Ошибка fetch для URL ${url}:`, error.message);
    throw error; 
  }
};

/**
 * Загружает список постов с пагинацией.
 */
export const fetchPostsAPI = async (page = 1) => {
  const skip = (page - 1) * POSTS_PER_PAGE;
  const url = `${API_BASE_URL}/posts/?skip=${skip}&limit=${POSTS_PER_PAGE}`;
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