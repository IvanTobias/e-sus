// /home/ubuntu/esus_project/frontend/src/components/ProntuarioSupport.js
import React, { useState, useEffect } from 'react';
import './ProntuarioSupport.css';

// Placeholder for content extracted/processed from the manual
const manualContent = {
  'default': [
    { title: 'Visão Geral do Prontuário (PEC)', content: 'O Prontuário Eletrônico do Cidadão (PEC) organiza as informações de saúde com base no método SOAP (Subjetivo, Objetivo, Avaliação, Plano). Inclui dados cadastrais, lista de problemas, notas de evolução e fichas de acompanhamento.' },
    { title: 'Método SOAP', content: 'Subjetivo: Queixa principal e história do paciente. Objetivo: Exame físico, sinais vitais, resultados de exames. Avaliação: Diagnósticos, problemas identificados (CIAP2/CID10). Plano: Condutas, prescrições, encaminhamentos, orientações.' },
  ],
  '2251': [ // Médico (Exemplo CBO Genérico)
    { title: 'SOAP - Médico', content: 'Registro completo do SOAP. Foco na Avaliação (diagnóstico diferencial, CID10) e Plano (prescrição, solicitação de exames complexos, encaminhamentos especializados).' },
    { title: 'Pré-Natal (Médico)', content: 'Acompanhamento de gestantes de risco habitual e alto risco. Interpretação de ultrassonografias, solicitação de exames específicos. Registro detalhado da DUM, IG, DPP. Avaliação de risco gestacional.' },
    { title: 'Puericultura (Médico)', content: 'Avaliação completa do crescimento e desenvolvimento. Investigação de alterações fenotípicas e fatores de risco. Interpretação de marcos do desenvolvimento e gráficos (Peso, Altura, PC, IMC).' },
    { title: 'Pessoa Idosa (Médico)', content: 'Aplicação e interpretação do IVCF-20. Avaliação multidimensional. Gerenciamento de polifarmácia e comorbidades.' },
  ],
  '2235': [ // Enfermeiro
    { title: 'SOAP - Enfermeiro', content: 'Registro completo do SOAP. Foco em cuidados de enfermagem, orientações, procedimentos (curativos, administração de medicação supervisionada), avaliação de sinais vitais e antropometria.' },
    { title: 'Pré-Natal (Enfermeiro)', content: 'Acompanhamento de gestantes de risco habitual. Realização de consultas alternadas com médico. Solicitação de exames de rotina. Orientações sobre amamentação e cuidados com o recém-nascido. Agendamento de consultas.' },
    { title: 'Puericultura (Enfermeiro)', content: 'Avaliação do crescimento (gráficos) e desenvolvimento (marcos). Orientações sobre aleitamento materno e introdução alimentar. Verificação do calendário vacinal.' },
    { title: 'Pessoa Idosa (Enfermeiro)', content: 'Aplicação do IVCF-20. Orientações sobre autocuidado, prevenção de quedas. Acompanhamento de condições crônicas.' },
    { title: 'Escuta Inicial / Acolhimento', content: 'Identificação da necessidade do usuário, classificação de risco (quando aplicável), coleta inicial de dados (queixa, sinais vitais básicos), direcionamento para atendimento.' },
  ],
  '3222': [ // Técnico/Auxiliar de Enfermagem
    { title: 'Procedimentos', content: 'Realização de procedimentos sob supervisão: curativos simples, administração de vacinas, verificação de sinais vitais, antropometria, glicemia capilar.' },
    { title: 'Escuta Inicial / Acolhimento', content: 'Coleta de dados iniciais, verificação de sinais vitais, organização do fluxo de atendimento.' },
    { title: 'Observação do Cidadão', content: 'Monitoramento de sinais vitais, administração de medicação prescrita, registro de intercorrências durante o período de observação, sob supervisão do enfermeiro ou médico.' },
  ],
  '2232': [ // Cirurgião-Dentista
    { title: 'SOAP - Odontologia', content: 'Registro específico com Odontograma. Subjetivo (queixa odontológica), Objetivo (exame intra/extra-bucal, necessidades especiais), Avaliação (CIAP2/CID10 odontológico, vigilância em saúde bucal, necessidade de prótese), Plano (Odontograma, Evoluções Odontológicas, procedimentos).' },
    { title: 'Odontograma', content: 'Registro visual da condição de cada dente (coroa e raiz) e faces. Uso de convenções (Cariado, Restaurado, Extraído, etc.). Registro de próteses, aparelhos, contenções.' },
    { title: 'Evoluções Odontológicas', content: 'Registro detalhado dos procedimentos realizados por dente, sextante ou arcada.' },
    { title: 'Tipos de Consulta Odontológica', content: '1ª Consulta (avaliação e plano), Consulta de Retorno (continuidade), Consulta de Manutenção (pós-tratamento).' },
  ],
  // Adicionar outros CBOs relevantes conforme necessidade (TSB, ASB, ACS, etc.)
};

// Lista de CBOs para o filtro (simplificada)
const cboOptions = [
  { value: 'default', label: 'Visão Geral' },
  { value: '2251', label: 'Médico' },
  { value: '2235', label: 'Enfermeiro' },
  { value: '3222', label: 'Técnico/Auxiliar de Enfermagem' },
  { value: '2232', label: 'Cirurgião-Dentista' },
  // Adicionar mais opções
];

function ProntuarioSupport() {
  const [selectedCbo, setSelectedCbo] = useState('default');
  const [content, setContent] = useState([]);

  useEffect(() => {
    // Carrega o conteúdo baseado no CBO selecionado
    const relevantContent = manualContent[selectedCbo] || manualContent['default'];
    setContent(relevantContent);
  }, [selectedCbo]);

  const handleCboChange = (event) => {
    setSelectedCbo(event.target.value);
  };

  return (
    <div className="prontuario-support-container">
      <h2>Apoio ao Preenchimento do Prontuário</h2>

      <div className="filter-section">
        <label htmlFor="cbo-select">Filtrar por Categoria Profissional (CBO):</label>
        <select id="cbo-select" value={selectedCbo} onChange={handleCboChange}>
          {cboOptions.map(option => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>

      <div className="content-section">
        {content.length > 0 ? (
          content.map((item, index) => (
            <div key={index} className="content-item">
              <h3>{item.title}</h3>
              <p>{item.content}</p>
            </div>
          ))
        ) : (
          <p>Nenhum conteúdo disponível para esta categoria.</p>
        )}
      </div>
    </div>
  );
}

export default ProntuarioSupport;

