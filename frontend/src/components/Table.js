// src/components/Table.js
import React from 'react';

function Table({ data }) {
  return (
    <table id="cadastrosTable">
      <thead>
        <tr>
          <th>Nome</th>
          <th>CPF</th>
          <th>CNS</th>
          <th>Data de Nascimento</th>
          <th>Unidade de Saúde</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, index) => (
          <tr key={index}>
            <td>{row.no_cidadao || 'N/A'}</td>
            <td>{row.nu_cpf || 'N/A'}</td>
            <td>{row.nu_cns || 'N/A'}</td>
            <td>{row.dt_nascimento || 'N/A'}</td>
            <td>{row.no_unidade_saude || 'N/A'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default Table;
