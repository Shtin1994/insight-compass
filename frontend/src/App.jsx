// frontend/src/App.jsx

import React, { useState } from 'react'; // <--- Импортируем useState
import './App.css';
import PostList from './components/PostList';
import CommentList from './components/CommentList'; // <--- Импортируем CommentList

function App() {
  const [selectedPostId, setSelectedPostId] = useState(null); // <--- Состояние для ID выбранного поста

  // Функция, которая будет передана в PostList, а затем в PostItem
  const handleShowComments = (postId) => {
    setSelectedPostId(postId); // Обновляем ID выбранного поста
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>Инсайт-Компас - Собранные данные</h1>
      </header>
      <main>
        {/* Передаем handleShowComments в PostList */}
        <PostList onPostSelect={handleShowComments} /> 
        
        {/* Отображаем CommentList, если пост выбран */}
        {selectedPostId && <CommentList postId={selectedPostId} />}
      </main>
    </div>
  );
}

export default App;