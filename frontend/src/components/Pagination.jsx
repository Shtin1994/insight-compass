// frontend/src/components/Pagination.jsx

import React from 'react';

function Pagination({ currentPage, totalPages, onPageChange }) {
  if (totalPages <= 1) {
    return null; // Не показываем пагинацию, если всего одна страница или меньше
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

  return (
    <div className="pagination" style={{ marginTop: '20px', textAlign: 'center' }}>
      <button onClick={handlePrevious} disabled={currentPage === 1}>
        « Назад
      </button>
      <span style={{ margin: '0 10px' }}>
        Страница {currentPage} из {totalPages}
      </span>
      <button onClick={handleNext} disabled={currentPage === totalPages}>
        Вперед »
      </button>
    </div>
  );
}

export default Pagination;