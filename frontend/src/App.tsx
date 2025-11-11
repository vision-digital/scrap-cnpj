import { useCallback, useEffect, useMemo, useState } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const datasetConfigs = {
  empresas: {
    label: 'Empresas',
    fields: [
      { name: 'razao_social', label: 'Razao social', placeholder: 'Nome parcial' },
      { name: 'natureza_juridica', label: 'Natureza juridica', placeholder: 'XXXX' },
      { name: 'porte', label: 'Porte', placeholder: '1/3/5' }
    ]
  },
  estabelecimentos: {
    label: 'Estabelecimentos',
    fields: [
      { name: 'cnpj', label: 'CNPJ', placeholder: '14 digitos' },
      { name: 'nome_fantasia', label: 'Nome fantasia', placeholder: 'Nome parcial' },
      { name: 'uf', label: 'UF', placeholder: 'SP' },
      { name: 'municipio', label: 'Municipio', placeholder: 'Codigo IBGE' },
      { name: 'cnae', label: 'CNAE', placeholder: '7 digitos' }
    ]
  },
  socios: {
    label: 'Socios',
    fields: [
      { name: 'cnpj_basico', label: 'CNPJ basico', placeholder: '8 digitos' },
      { name: 'nome', label: 'Nome socio', placeholder: 'Nome parcial' }
    ]
  },
  simples: {
    label: 'Simples',
    fields: [
      { name: 'cnpj_basico', label: 'CNPJ basico', placeholder: '8 digitos' },
      { name: 'opcao_simples', label: 'Opcao Simples', placeholder: 'S/N' },
      { name: 'opcao_mei', label: 'Opcao MEI', placeholder: 'S/N' }
    ]
  }
} as const;

const headers = {
  empresas: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'porte_empresa', 'capital_social'],
  estabelecimentos: ['cnpj14', 'nome_fantasia', 'uf', 'municipio', 'cnae_fiscal_principal'],
  socios: ['cnpj_basico', 'nome_socio', 'cnpj_cpf_socio', 'codigo_qualificacao_socio'],
  simples: ['cnpj_basico', 'opcao_simples', 'opcao_mei', 'data_opcao_simples']
};

type DatasetKey = keyof typeof datasetConfigs;

type SearchResponse = {
  total: number;
  page: number;
  page_size: number;
  items: Record<string, string | number | null>[];
};

function App() {
  const [dataset, setDataset] = useState<DatasetKey>('empresas');
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [data, setData] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(25);
  const [updateStatus, setUpdateStatus] = useState<string>('Desconhecido');
  const [manualRelease, setManualRelease] = useState('');

  const datasetFields = useMemo(() => datasetConfigs[dataset].fields, [dataset]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await axios.get<SearchResponse>(`${API_URL}/api/search/${dataset}`, {
        params: {
          ...filters,
          page,
          page_size: pageSize
        }
      });
      setData(response.data);
    } catch (err) {
      setError('Falha ao buscar dados. Confira filtros e conexao com a API.');
    } finally {
      setLoading(false);
    }
  }, [dataset, filters, page, pageSize]);

  const fetchStatus = useCallback(async () => {
    try {
      const response = await axios.get(`${API_URL}/api/updates/status`);
      const payload = response.data;
      if (payload.release) {
        setUpdateStatus(`${payload.release} - ${payload.status}`);
      } else {
        setUpdateStatus('Sem historico');
      }
    } catch {
      setUpdateStatus('Indisponivel');
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  const handleFilterChange = (name: string, value: string) => {
    setFilters((prev) => ({ ...prev, [name]: value }));
  };

  const handleExport = () => {
    const params = new URLSearchParams({ ...filters });
    const query = params.toString();
    const url = `${API_URL}/api/export/${dataset}${query ? `?${query}` : ''}`;
    window.open(url, '_blank');
  };

  const handleUpdate = async () => {
    try {
      await axios.post(`${API_URL}/api/updates/run`, {
        release: manualRelease || undefined
      });
      setManualRelease('');
      fetchStatus();
      alert('Atualizacao iniciada. Consulte o status periodicamente.');
    } catch {
      alert('Nao foi possivel iniciar a atualizacao. Verifique os logs do backend.');
    }
  };

  return (
    <div className="page">
      <header>
        <div>
          <h1>Scrap CNPJ</h1>
          <p>Consulta rapida aos dados abertos do CNPJ com atualizacao guiada.</p>
        </div>
        <div className="status">
          <span>Versao atual:</span>
          <strong>{updateStatus}</strong>
        </div>
      </header>

      <section className="controls">
        <div className="dataset-selector">
          {(Object.keys(datasetConfigs) as DatasetKey[]).map((item) => (
            <button
              key={item}
              className={item === dataset ? 'active' : ''}
              onClick={() => {
                setDataset(item);
                setFilters({});
                setPage(1);
              }}
            >
              {datasetConfigs[item].label}
            </button>
          ))}
        </div>

        <div className="filters">
          {datasetFields.map((field) => (
            <label key={field.name}>
              {field.label}
              <input
                name={field.name}
                placeholder={field.placeholder}
                value={filters[field.name] || ''}
                onChange={(event) => handleFilterChange(field.name, event.target.value)}
              />
            </label>
          ))}
          <div className="filter-actions">
            <button onClick={fetchData} disabled={loading}>
              {loading ? 'Carregando...' : 'Buscar'}
            </button>
            <button onClick={handleExport}>Exportar CSV</button>
          </div>
        </div>
      </section>

      <section className="update-panel">
        <h2>Atualizacao da base</h2>
        <div className="update-actions">
          <input
            placeholder="Release especifica (YYYY-MM) opcional"
            value={manualRelease}
            onChange={(event) => setManualRelease(event.target.value)}
          />
          <button onClick={handleUpdate}>Verificar novidades</button>
        </div>
      </section>

      <section className="results">
        <div className="results-header">
          <h2>Resultados</h2>
          <span>{data?.total ?? 0} registros</span>
        </div>
        {error && <p className="error">{error}</p>}
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                {headers[dataset].map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data?.items.map((item, index) => (
                <tr key={index}>
                  {headers[dataset].map((column) => (
                    <td key={column}>{item[column] ?? '-'}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="pagination">
          <button disabled={page === 1} onClick={() => setPage((prev) => Math.max(1, prev - 1))}>
            Anterior
          </button>
          <span>Pagina {page}</span>
          <button onClick={() => setPage((prev) => prev + 1)}>Proxima</button>
        </div>
      </section>
    </div>
  );
}

export default App;
