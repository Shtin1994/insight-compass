// frontend/src/components/Pagination.jsx
import React, { useState, useEffect } from 'react';

function Pagination({ currentPage, totalPages, onPageChange }) {
  const [inputPage, setInputPage] = useState(currentPage.toString());

  // Обновляем inputPage, если currentPage изменился извне (например, кнопками "Назад/Вперед")
  useEffect(() => {
    setInputPage(currentPage.toString());
  }, [currentPage]);

  if (totalPages <= 1) {
    return null;
  }

  const handlePrevious = () => {
    if (currentPage > 1) {
      onPageChange(currentPage - 1);
    }
  };

  const handleNext = () => {
    if (currentPage < totalPages) {
      onPageChange(currentPage + 1);
    }
  };

  const handlePageClick = (pageNumber) => {
    if (pageNumber !== currentPage) {
      onPageChange(pageNumber);
    }
  };

  const handleInputChange = (e) => {
    setInputPage(e.target.value);
  };

  const handleInputSubmit = (e) => {
    e.preventDefault(); // Если это часть формы
    const pageNum = parseInt(inputPage, 10);
    if (!isNaN(pageNum) && pageNum >= 1 && pageNum <= totalPages) {
      if (pageNum !== currentPage) {
        onPageChange(pageNum);
      }
    } else {
      // Если введено некорректное значение, можно сбросить на текущую страницу
      // или показать ошибку (пока просто сбрасываем на текущую)
      setInputPage(currentPage.toString());
      alert(`Пожалуйста, введите номер страницы от 1 до ${totalPages}.`);
    }
  };

  const pageNumbers = [];
  const maxPagesToShow = 5;
  const halfPagesToShow = Math.floor(maxPagesToShow / 2);

  if (totalPages > 0) {
    pageNumbers.push(1);
  }
  
  let startPage = Math.max(2, currentPage - halfPagesToShow);
  let endPage = Math.min(totalPages - 1, currentPage + halfPagesToShow);

  if (currentPage - halfPagesToShow < 2) {
      endPage = Math.min(totalPages - 1, startPage + maxPagesToShow - (currentPage <= 2 ? 2 : 1) );
  }
  if (currentPage + halfPagesToShow > totalPages - 1) {
      startPage = Math.max(2, endPage - maxPagesToShow + (currentPage >= totalPages -1 ? 2:1) );
  }

  if (startPage > 2) {
    pageNumbers.push('...');
  }

  for (let i = startPage; i <= endPage; i++) {
    if (i > 1 && i < totalPages) {
        pageNumbers.push(i);
    }
  }

  if (endPage < totalPages - 1) {
    pageNumbers.push('...');
  }

  if (totalPages > 1) {
    pageNumbers.push(totalPages);
  }
  
  const uniquePageNumbers = [];
  let lastPushed = null;
  for (const p of pageNumbers) {
      if (p === '...' && lastPushed === '...') {
          continue;
      }
      // Избавимся от дублирования номеров страниц, если 1 или totalPages попали в основной диапазон
      if (typeof p === 'number' && uniquePageNumbers.includes(p)) {
          continue;
      }
      uniquePageNumbers.push(p);
      lastPushed = p;
  }

  return (
    <div className="pagination" style={{ marginTop: '20px', textAlign: 'center', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
      <button onClick={handlePrevious} disabled={currentPage === 1} style={{ marginRight: '5px' }}>
        « Назад
      </button>

      {uniquePageNumbers.map((page, index) =>
        typeof page === 'number' ? (
          <button
            key={index}
            onClick={() => handlePageClick(page)}
            disabled={currentPage === page}
            style={{
              fontWeight: currentPage === page ? 'bold' : 'normal',
              margin: '0 3px',
              minWidth: '30px',
              padding: '5px 8px'
            }}
          >
            {page}
          </button>
        ) : (
          <span key={index} style={{ margin: '0 5px', cursor: 'default', padding: '5px 0' }}>
            {page}
          </span>
        )
      )}

      <button onClick={handleNext} disabled={currentPage === totalPages} style={{ marginLeft: '5px' }}>
        Вперед »
      </button>

      {/* Форма для ввода номера страницы */}
      <form onSubmit={handleInputSubmit} style={{ display: 'inline-flex', alignItems: 'center', marginLeft: '15px' }}>
        <input
          type="number"
          value={inputPage}
          onChange={handleInputChange}
          min="1"
          max={totalPages}
          style={{ width: '60px', textAlign: 'center', marginRight: '5px', padding: '6px' }}
          aria-label="Номер страницы"
        />
        <button type="submit" style={{padding: '6px 10px'}}>Перейти</button>
      </form>
      
      <span style={{ marginLeft: '15px', fontSize: '0.9em', color: '#666' }}>
        (Стр. {currentPage} из {totalPages})
      </span>
    </div>
  );
}

export default Pagination;