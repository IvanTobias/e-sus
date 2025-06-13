//Fiocruzintegrationpage.js

import React, { useState, useEffect } from 'react';
import { Card, Button, ProgressBar, Alert, Form, Container, Row, Col } from 'react-bootstrap';
import { Database, FileText, CheckCircle, AlertTriangle } from 'react-feather';
import axios from 'axios';
import io from 'socket.io-client';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || `http://${window.location.hostname}:5000`;


const FiocruzIntegrationPage = () => {
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState('idle'); // idle, running, success, error
  const [message, setMessage] = useState('');
  const [exportParquet, setExportParquet] = useState(true);
  const [parquetDir, setParquetDir] = useState('./input');
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    const socket = io(API_BASE_URL);

    socket.on('fiocruz_integration_progress', (data) => {
      if (data.progress !== undefined) {
        setProgress(data.progress);
      }

      if (data.message) {
        setLogs(prevLogs => [...prevLogs, data.message]);
        setMessage(data.message);
      }

      if (data.error) {
        setLogs(prevLogs => [...prevLogs, `Erro: ${data.error}`]);
        setMessage(data.error);
        setStatus('error');
      }

      if (data.progress === 100) {
        setStatus('success');
      }
    });

    return () => {
      socket.disconnect();
    };
  }, []);


  // Função para iniciar a integração
  const startIntegration = async () => {
    try {
      setStatus('running');
      setProgress(0);
      setMessage('Iniciando integração Fiocruz...');
      setLogs(['Iniciando integração Fiocruz...']);

      const response = await axios.post(`${API_BASE_URL}/api/v1/fiocruz/execute-integration`, {
        exportParquet,
        parquetDir
      });

      if (response.data.success) {
        setStatus('success');
        setMessage('Integração concluída com sucesso!');
      } else {
        setStatus('error');
        setMessage(`Erro: ${response.data.error}`);
      }
    } catch (error) {
      setStatus('error');
      setMessage(`Erro: ${error.response?.data?.error || error.message}`);
      console.error('Erro ao executar integração:', error);
    }
  };

  // Renderizar indicador de status
  const renderStatusIndicator = () => {
    switch (status) {
      case 'running':
        return (
          <div className="mb-4">
            <ProgressBar animated now={progress} label={`${progress}%`} />
            <small className="text-muted mt-2 d-block">Progresso: {progress}%</small>
          </div>
        );
      case 'success':
        return (
          <Alert variant="success" className="d-flex align-items-center">
            <CheckCircle className="mr-2" size={20} />
            <span>{message}</span>
          </Alert>
        );
      case 'error':
        return (
          <Alert variant="danger" className="d-flex align-items-center">
            <AlertTriangle className="mr-2" size={20} />
            <span>{message}</span>
          </Alert>
        );
      default:
        return null;
    }
  };

  return (
    <Container fluid className="p-4">
      <h2 className="mb-4">
        <Database className="mr-2" size={24} />
        Integração Fiocruz
      </h2>
      
      <Row>
        <Col md={8}>
          <Card className="mb-4">
            <Card.Header>
              <h5 className="mb-0">Configuração da Integração</h5>
            </Card.Header>
            <Card.Body>
              <p>
                Esta ferramenta realiza a integração híbrida SQL + Polars para o sistema e-SUS/painel-esus da Fiocruz.
                O processo inclui:
              </p>
              <ul>
                <li>Criação de tabelas intermediárias usando SQL</li>
                <li>Processamento de dados complexos com Polars</li>
                <li>Exportação de dados em formato Parquet</li>
                <li>Geração de listas nominais</li>
              </ul>

              <Form>
                <Form.Group className="mb-3">
                  <Form.Check 
                    type="checkbox" 
                    label="Exportar dados para arquivos Parquet" 
                    checked={exportParquet}
                    onChange={(e) => setExportParquet(e.target.checked)}
                  />
                </Form.Group>

                {exportParquet && (
                  <Form.Group className="mb-3">
                    <Form.Label>Diretório para arquivos Parquet</Form.Label>
                    <Form.Control 
                      type="text" 
                      value={parquetDir}
                      onChange={(e) => setParquetDir(e.target.value)}
                      placeholder="./input"
                    />
                    <Form.Text className="text-muted">
                      Diretório onde os arquivos Parquet serão salvos. Será criado se não existir.
                    </Form.Text>
                  </Form.Group>
                )}

                <Button 
                  variant="primary" 
                  onClick={startIntegration}
                  disabled={status === 'running'}
                  className="mt-3"
                >
                  <FileText className="mr-2" size={16} />
                  {status === 'running' ? 'Executando...' : 'Iniciar Integração Fiocruz'}
                </Button>
              </Form>

              {renderStatusIndicator()}
            </Card.Body>
          </Card>
        </Col>

        <Col md={4}>
          <Card className="mb-4">
            <Card.Header>
              <h5 className="mb-0">Logs de Execução</h5>
            </Card.Header>
            <Card.Body>
              <div 
                style={{ 
                  height: '400px', 
                  overflowY: 'auto', 
                  backgroundColor: '#f8f9fa',
                  padding: '10px',
                  borderRadius: '4px',
                  fontFamily: 'monospace',
                  fontSize: '0.9rem'
                }}
              >
                {logs.map((log, index) => (
                  <div key={index} className="mb-1">
                    {log}
                  </div>
                ))}
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>

      <Row>
        <Col>
          <Card>
            <Card.Header>
              <h5 className="mb-0">Arquivos Gerados</h5>
            </Card.Header>
            <Card.Body>
              <p>
                Após a conclusão da integração, os seguintes arquivos serão gerados:
              </p>
              <ul>
                <li><strong>Tabelas no Banco Local:</strong> pessoas, diabetes_nominal, crianca, idoso, etc.</li>
                <li><strong>Arquivos Parquet:</strong> tb_dim_cbo.parquet, tb_fat_atendimento_individual.parquet, etc.</li>
                <li><strong>Listas Nominais:</strong> nominal_list, visita_acs, nominal_list_atendimentos_medicos, etc.</li>
              </ul>
              <p>
                Estes arquivos são compatíveis com o sistema original da Fiocruz e podem ser utilizados para análise e visualização de dados.
              </p>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default FiocruzIntegrationPage;
