// src/components/DashboardPage.js
import React, { useState, useEffect } from 'react';
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import axios from 'axios'; // Import axios for API calls

// Helper function to get API base URL
const getApiBaseUrl = () => {
  // Use localhost:5000 for development, adjust if needed for production
  return `http://${window.location.hostname}:5000/api/v1`;
};

// --- Component --- 
function DashboardPage() {
  // State for different dashboard data
  const [diabetesTotal, setDiabetesTotal] = useState(null);
  const [hypertensionAgeGender, setHypertensionAgeGender] = useState(null);
  const [oralHealthTotal, setOralHealthTotal] = useState(null);
  // Add more states for other Fiocruz dashboards as needed

  const [loading, setLoading] = useState(true); // Start with loading true
  const [error, setError] = useState(null); // Error state

  // --- Data Fetching Logic for Fiocruz Dashboards ---
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      const baseUrl = getApiBaseUrl();
      
      try {
        // Fetch data for different dashboards concurrently
        const [diabetesRes, hypertensionRes, oralHealthRes] = await Promise.all([
          axios.get(`${baseUrl}/diabetes/total`), // Example: Diabetes total
          axios.get(`${baseUrl}/hypertension/age-group-gender`), // Example: Hypertension age/gender
          axios.get(`${baseUrl}/oral-health/total`), // Example: Oral Health total
          // Add more axios calls for other Fiocruz endpoints here
        ]);

        // Process and set state for Diabetes Total
        if (diabetesRes.data && diabetesRes.data.data && diabetesRes.data.data.length > 0) {
          // Assuming the API returns an array like [{ "Total": 123 }]
          setDiabetesTotal(diabetesRes.data.data[0].Total || 0);
        } else {
           setDiabetesTotal(0); // Default or handle no data
        }

        // Process and set state for Hypertension Age/Gender
        if (hypertensionRes.data && hypertensionRes.data.data) {
           // Assuming data format is like [{ faixa_etaria: '20-39', Masculino: 50, Feminino: 60 }, ...]
           // Transform data if needed for the chart
           setHypertensionAgeGender(hypertensionRes.data.data);
        } else {
           setHypertensionAgeGender([]);
        }

        // Process and set state for Oral Health Total
        if (oralHealthRes.data && oralHealthRes.data.data && oralHealthRes.data.data.length > 0) {
          // Assuming the API returns an array like [{ "Total": 456 }]
          setOralHealthTotal(oralHealthRes.data.data[0].Total || 0);
        } else {
           setOralHealthTotal(0); // Default or handle no data
        }

        // Process other responses here...

      } catch (err) {
        console.error("Error fetching Fiocruz dashboard data:", err);
        setError("Falha ao carregar dados dos dashboards da Fiocruz. Verifique se o backend está rodando e acessível.");
        // Clear data on error
        setDiabetesTotal(null);
        setHypertensionAgeGender(null);
        setOralHealthTotal(null);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []); // Empty dependency array means this runs once on mount

  // --- Render Logic ---
  return (
    <div className="container">
      <h1>Dashboards e-SUS (Fiocruz)</h1>
      <p>Visualização de dados integrados do painel da Fiocruz.</p>

      {loading && <p>Carregando dashboards...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}

      {!loading && !error && (
        <div className="dashboard-grid"> 
          
          {/* --- Diabetes Total Example --- */}
          <div className="dashboard-chart-container simple-kpi">
            <h2>Total de Pacientes Diabéticos</h2>
            {diabetesTotal !== null ? (
              <p className="kpi-value">{diabetesTotal}</p>
            ) : (
              <p>Dados indisponíveis</p>
            )}
          </div>

          {/* --- Hypertension Age/Gender Example --- */}
          <div className="dashboard-chart-container">
            <h2>Hipertensos por Faixa Etária e Sexo</h2>
            {hypertensionAgeGender && hypertensionAgeGender.length > 0 ? (
              <div style={{ width: '100%', height: 300 }}>
                <ResponsiveContainer>
                  <BarChart data={hypertensionAgeGender} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="faixa_etaria" name="Faixa Etária"/>
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="Masculino" fill="#0088FE" name="Masculino" />
                    <Bar dataKey="Feminino" fill="#FF8042" name="Feminino" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p>Dados indisponíveis</p>
            )}
          </div>

          {/* --- Oral Health Total Example --- */}
          <div className="dashboard-chart-container simple-kpi">
            <h2>Total de Atendimentos Odontológicos</h2>
            {oralHealthTotal !== null ? (
              <p className="kpi-value">{oralHealthTotal}</p>
            ) : (
              <p>Dados indisponíveis</p>
            )}
          </div>

          {/* Add more charts here based on other fetched data */}
          {/* Example: 
          <div className="dashboard-chart-container">
            <h2>Another Fiocruz Chart</h2>
            {anotherData && anotherData.length > 0 ? (
              // ... Recharts component using anotherData ...
            ) : (
              <p>Dados indisponíveis</p>
            )}
          </div>
          */}

        </div>
      )}
    </div>
  );
}

export default DashboardPage;

