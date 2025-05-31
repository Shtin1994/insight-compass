// frontend/src/components/Modal.jsx
import React from 'react';
import './Modal.css'; // Мы создадим этот CSS файл для стилей

function Modal({ isOpen, onClose, title, children }) {
  if (!isOpen) {
    return null;
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="modal-title">{title || 'Детали'}</h3>
          <button className="modal-close-button" onClick={onClose}>
            × {/* Крестик */}
          </button>
        </div>
        <div className="modal-body">
          {children}
        </div>
        {/* Можно добавить футер с кнопками, если нужно */}
        {/* <div className="modal-footer">
          <button onClick={onClose}>Закрыть</button>
        </div> */}
      </div>
    </div>
  );
}

export default Modal;