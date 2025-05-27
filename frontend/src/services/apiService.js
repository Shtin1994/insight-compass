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
      // Попытаемся получить детали ошибки из тела ответа, если возможно
      let errorDetail = `Ошибка HTTP: ${response.status} ${response.statusText}`;
      try {
        const errorData = await response.json();
        errorDetail = errorData.detail || errorDetail;
      } catch (e) {
        // Не удалось распарсить JSON, используем стандартное сообщение
      }
      throw new Error(errorDetail);
    }
    return await response.json();
  } catch (error) {
    console.error(`Ошибка fetch для URL ${url}:`, error.message);
    // Перебрасываем ошибку дальше, чтобы компонент мог ее обработать
    throw error; 
  }
};

/**
 * Загружает список постов с пагинацией.
 * @param {number} page - Номер страницы (начиная с 1).
 * @returns {Promise<{total_posts: number, posts: Array<object>}>}
 */
export const fetchPostsAPI = async (page = 1) => {
  const skip = (page - 1) * POSTS_PER_PAGE;
  const url = `${API_BASE_URL}/posts/?skip=${skip}&limit=${POSTS_PER_PAGE}`;
  return fetchData(url);
};

/**
 * Загружает комментарии для указанного поста с пагинацией.
 * @param {number} postId - ID поста.
 * @param {number} page - Номер страницы комментариев (начиная с 1).
 * @returns {Promise<{total_comments: number, comments: Array<object>}>}
 */
export const fetchCommentsAPI = async (postId, page = 1) => {
  const skip = (page - 1) * COMMENTS_PER_PAGE;
  const url = `${API_BASE_URL}/posts/${postId}/comments/?skip=${skip}&limit=${COMMENTS_PER_PAGE}`;
  return fetchData(url);
};