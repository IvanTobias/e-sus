// src/components/Pagination.js
import React from 'react';

function Pagination({ currentPage, setCurrentPage, hasNextPage }) {
  return (
    <div className="pagination">
      <button onClick={() => setCurrentPage((prev) => prev - 1)} disabled={currentPage === 1}>Anterior</button>
      <button onClick={() => setCurrentPage((prev) => prev + 1)} disabled={!hasNextPage}>Próxima</button>
    </div>
  );
}

export default Pagination;
