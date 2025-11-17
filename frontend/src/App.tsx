import { useCallback, useEffect, useState } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// Estados brasileiros
const ESTADOS_BR = [
  { uf: 'AC', nome: 'Acre' },
  { uf: 'AL', nome: 'Alagoas' },
  { uf: 'AP', nome: 'Amapá' },
  { uf: 'AM', nome: 'Amazonas' },
  { uf: 'BA', nome: 'Bahia' },
  { uf: 'CE', nome: 'Ceará' },
  { uf: 'DF', nome: 'Distrito Federal' },
  { uf: 'ES', nome: 'Espírito Santo' },
  { uf: 'GO', nome: 'Goiás' },
  { uf: 'MA', nome: 'Maranhão' },
  { uf: 'MT', nome: 'Mato Grosso' },
  { uf: 'MS', nome: 'Mato Grosso do Sul' },
  { uf: 'MG', nome: 'Minas Gerais' },
  { uf: 'PA', nome: 'Pará' },
  { uf: 'PB', nome: 'Paraíba' },
  { uf: 'PR', nome: 'Paraná' },
  { uf: 'PE', nome: 'Pernambuco' },
  { uf: 'PI', nome: 'Piauí' },
  { uf: 'RJ', nome: 'Rio de Janeiro' },
  { uf: 'RN', nome: 'Rio Grande do Norte' },
  { uf: 'RS', nome: 'Rio Grande do Sul' },
  { uf: 'RO', nome: 'Rondônia' },
  { uf: 'RR', nome: 'Roraima' },
  { uf: 'SC', nome: 'Santa Catarina' },
  { uf: 'SP', nome: 'São Paulo' },
  { uf: 'SE', nome: 'Sergipe' },
  { uf: 'TO', nome: 'Tocantins' },
];

// Situações cadastrais
const SITUACOES = [
  { codigo: '01', nome: '01 - NULA' },
  { codigo: '2', nome: '2 - ATIVA' },
  { codigo: '3', nome: '3 - SUSPENSA' },
  { codigo: '4', nome: '4 - INAPTA' },
  { codigo: '08', nome: '08 - BAIXADA' },
];

type SearchResponse = {
  total: number;
  page: number;
  page_size: number;
  has_more?: boolean;  // Indicates if there are more pages
  items: Record<string, string | number | null>[];
};

type Municipio = {
  codigo: string;
  descricao: string;
};

type Cnae = {
  codigo: string;
  descricao: string;
};

function App() {
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [data, setData] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(25);
  const [updateStatus, setUpdateStatus] = useState<string>('Desconhecido');
  const [manualRelease, setManualRelease] = useState('');

  // Modal de municípios
  const [showMunicipioModal, setShowMunicipioModal] = useState(false);
  const [municipioSearch, setMunicipioSearch] = useState('');
  const [municipios, setMunicipios] = useState<Municipio[]>([]);
  const [selectedMunicipio, setSelectedMunicipio] = useState<Municipio | null>(null);
  const [loadingMunicipios, setLoadingMunicipios] = useState(false);

  // Modal de CNAEs
  const [showCnaeModal, setShowCnaeModal] = useState(false);
  const [cnaeSearch, setCnaeSearch] = useState('');
  const [cnaes, setCnaes] = useState<Cnae[]>([]);
  const [selectedCnaes, setSelectedCnaes] = useState<Cnae[]>([]);
  const [loadingCnaes, setLoadingCnaes] = useState(false);

  // Modal de sócios
  const [selectedCnpj, setSelectedCnpj] = useState<string | null>(null);
  const [sociosData, setSociosData] = useState<SearchResponse | null>(null);
  const [selectedSocio, setSelectedSocio] = useState<string | null>(null);
  const [socioEmpresasData, setSocioEmpresasData] = useState<SearchResponse | null>(null);
  const [loadingModal, setLoadingModal] = useState(false);

  // Auxiliares
  const [loadingAuxiliares, setLoadingAuxiliares] = useState(false);

  const hasFilters = () => {
    // Verifica se há pelo menos um filtro aplicado
    const hasBasicFilters = Object.keys(filters).some(key => filters[key] && filters[key].trim() !== '');
    const hasMunicipio = selectedMunicipio !== null;
    const hasCnaes = selectedCnaes.length > 0;
    return hasBasicFilters || hasMunicipio || hasCnaes;
  };

  const fetchData = async () => {
    // Validar se há filtros antes de fazer a busca
    if (!hasFilters()) {
      setError('Por favor, selecione pelo menos um filtro antes de buscar.');
      setData(null);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      // Build query params manually to handle CNAE array correctly (FastAPI expects cnae=val1&cnae=val2, not cnae[]=val1)
      const params = new URLSearchParams();

      // Add regular filters
      Object.entries(filters).forEach(([key, value]) => {
        if (value) params.append(key, value);
      });

      // Add municipio if selected
      if (selectedMunicipio) params.append('municipio', selectedMunicipio.codigo);

      // Add CNAEs (append multiple times for array)
      selectedCnaes.forEach(cnae => params.append('cnae', cnae.codigo));

      // Add pagination
      params.append('page', String(page));
      params.append('page_size', String(pageSize));

      const response = await axios.get<SearchResponse>(`${API_URL}/api/search/estabelecimentos?${params.toString()}`);
      setData(response.data);
    } catch (err) {
      setError('Falha ao buscar dados. Confira filtros e conexão com a API.');
    } finally {
      setLoading(false);
    }
  };

  const fetchStatus = useCallback(async () => {
    try {
      const response = await axios.get(`${API_URL}/api/updates/status`);
      const payload = response.data;
      if (payload.release) {
        setUpdateStatus(`${payload.release} - ${payload.status}`);
      } else {
        setUpdateStatus('Sem histórico');
      }
    } catch {
      setUpdateStatus('Indisponível');
    }
  }, []);

  // Removed automatic fetchData on mount/filter change - user must click "Buscar" button

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  const handleFilterChange = (name: string, value: string) => {
    setFilters((prev) => ({ ...prev, [name]: value }));
  };

  const handleExport = () => {
    const params = new URLSearchParams({ ...filters });
    if (selectedMunicipio) params.append('municipio', selectedMunicipio.codigo);
    selectedCnaes.forEach(cnae => params.append('cnae', cnae.codigo));
    const query = params.toString();
    const url = `${API_URL}/api/export/estabelecimentos${query ? `?${query}` : ''}`;
    window.open(url, '_blank');
  };

  const handleUpdate = async () => {
    try {
      await axios.post(`${API_URL}/api/updates/run`, {
        release: manualRelease || undefined
      });
      setManualRelease('');
      fetchStatus();
      alert('Atualização iniciada. Consulte o status periodicamente.');
    } catch {
      alert('Não foi possível iniciar a atualização. Verifique os logs do backend.');
    }
  };

  const handleLoadAuxiliares = async () => {
    setLoadingAuxiliares(true);
    try {
      await axios.post(`${API_URL}/api/auxiliares/load`);
      alert('Tabelas auxiliares carregadas com sucesso!');
    } catch (err) {
      alert('Erro ao carregar tabelas auxiliares. Verifique os logs.');
    } finally {
      setLoadingAuxiliares(false);
    }
  };

  const searchMunicipios = async () => {
    if (municipioSearch.length < 2) return;
    setLoadingMunicipios(true);
    try {
      const response = await axios.get<Municipio[]>(`${API_URL}/api/auxiliares/municipios`, {
        params: { q: municipioSearch, limit: 50 }
      });
      setMunicipios(response.data);
    } catch {
      alert('Erro ao buscar municípios');
    } finally {
      setLoadingMunicipios(false);
    }
  };

  const searchCnaes = async () => {
    if (cnaeSearch.length < 2) return;
    setLoadingCnaes(true);
    try {
      const response = await axios.get<Cnae[]>(`${API_URL}/api/auxiliares/cnaes`, {
        params: { q: cnaeSearch, limit: 50 }
      });
      setCnaes(response.data);
    } catch {
      alert('Erro ao buscar CNAEs');
    } finally {
      setLoadingCnaes(false);
    }
  };

  const handleCnpjClick = async (cnpjBasico: string) => {
    // Close any open modal before opening the new one
    setSelectedSocio(null);
    setSocioEmpresasData(null);

    setSelectedCnpj(cnpjBasico);
    setLoadingModal(true);
    try {
      const response = await axios.get<SearchResponse>(`${API_URL}/api/search/cnpj/${cnpjBasico}/socios`);
      setSociosData(response.data);
    } catch {
      alert('Erro ao buscar sócios.');
    } finally {
      setLoadingModal(false);
    }
  };

  const handleSocioClick = async (cpfCnpj: string) => {
    setSelectedSocio(cpfCnpj);
    setLoadingModal(true);
    try {
      const response = await axios.get<SearchResponse>(`${API_URL}/api/search/socio/${cpfCnpj}/empresas`);
      setSocioEmpresasData(response.data);
    } catch {
      alert('Erro ao buscar empresas do sócio.');
    } finally {
      setLoadingModal(false);
    }
  };

  const closeModal = () => {
    setSelectedCnpj(null);
    setSociosData(null);
    setSelectedSocio(null);
    setSocioEmpresasData(null);
  };

  return (
    <div className="page">
      <header>
        <div>
          <h1>Scrap CNPJ</h1>
          <p>Consulta rápida aos dados abertos do CNPJ</p>
        </div>
        <div className="status">
          <span>Versão:</span>
          <strong>{updateStatus}</strong>
        </div>
      </header>

      <section className="controls">
        <h2>Filtros de Estabelecimentos</h2>
        <div className="filters">
          <label>
            CNPJ Completo
            <input
              placeholder="14 dígitos"
              value={filters.cnpj || ''}
              onChange={(e) => handleFilterChange('cnpj', e.target.value)}
            />
          </label>

          <label>
            CNPJ Básico
            <input
              placeholder="8 dígitos"
              value={filters.cnpj_basico || ''}
              onChange={(e) => handleFilterChange('cnpj_basico', e.target.value)}
            />
          </label>

          <label>
            Nome Fantasia
            <input
              placeholder="Nome parcial"
              value={filters.nome_fantasia || ''}
              onChange={(e) => handleFilterChange('nome_fantasia', e.target.value)}
            />
          </label>

          <label>
            Situação Cadastral
            <select
              value={filters.situacao_cadastral || ''}
              onChange={(e) => handleFilterChange('situacao_cadastral', e.target.value)}
            >
              <option value="">Todas</option>
              {SITUACOES.map((sit) => (
                <option key={sit.codigo} value={sit.codigo}>
                  {sit.nome}
                </option>
              ))}
            </select>
          </label>

          <label>
            UF
            <select
              value={filters.uf || ''}
              onChange={(e) => handleFilterChange('uf', e.target.value)}
            >
              <option value="">Todos</option>
              {ESTADOS_BR.map((estado) => (
                <option key={estado.uf} value={estado.uf}>
                  {estado.uf} - {estado.nome}
                </option>
              ))}
            </select>
          </label>

          <label>
            Município
            <div className="input-with-button">
              <input
                readOnly
                placeholder="Clique para buscar"
                value={selectedMunicipio?.descricao || ''}
                onClick={() => setShowMunicipioModal(true)}
              />
              {selectedMunicipio && (
                <button className="clear-btn" onClick={() => setSelectedMunicipio(null)}>
                  ✕
                </button>
              )}
            </div>
          </label>

          <label>
            CNAE
            <div className="input-with-button">
              <input
                readOnly
                placeholder="Clique para buscar"
                value={selectedCnaes.map(c => `${c.codigo} - ${c.descricao}`).join(', ') || ''}
                onClick={() => setShowCnaeModal(true)}
              />
              {selectedCnaes.length > 0 && (
                <button className="clear-btn" onClick={() => setSelectedCnaes([])}>
                  ✕
                </button>
              )}
            </div>
          </label>

          <label>
            Bairro
            <input
              placeholder="Nome parcial"
              value={filters.bairro || ''}
              onChange={(e) => handleFilterChange('bairro', e.target.value)}
            />
          </label>

          <label>
            Logradouro
            <input
              placeholder="Rua/Av parcial"
              value={filters.logradouro || ''}
              onChange={(e) => handleFilterChange('logradouro', e.target.value)}
            />
          </label>

          <label>
            CEP
            <input
              placeholder="8 dígitos"
              value={filters.cep || ''}
              onChange={(e) => handleFilterChange('cep', e.target.value)}
            />
          </label>

          <label>
            Tipo
            <select
              value={filters.matriz_filial || ''}
              onChange={(e) => handleFilterChange('matriz_filial', e.target.value)}
            >
              <option value="">Todos</option>
              <option value="1">Matriz</option>
              <option value="2">Filial</option>
            </select>
          </label>

          <div className="filter-actions">
            <button onClick={fetchData} disabled={loading || !hasFilters()}>
              {loading ? 'Carregando...' : 'Buscar'}
            </button>
            <button onClick={handleExport} className="export-btn" disabled={!hasFilters()}>
              Exportar CSV
            </button>
          </div>
        </div>
      </section>

      <section className="update-panel">
        <h2>Atualizações</h2>
        <div className="update-actions">
          <input
            placeholder="Release (YYYY-MM) opcional"
            value={manualRelease}
            onChange={(e) => setManualRelease(e.target.value)}
          />
          <button onClick={handleUpdate}>Atualizar Dados Principais</button>
          <button onClick={handleLoadAuxiliares} disabled={loadingAuxiliares} className="aux-btn">
            {loadingAuxiliares ? 'Carregando...' : 'Carregar Tabelas Auxiliares'}
          </button>
        </div>
      </section>

      <section className="results">
        <div className="results-header">
          <h2>Resultados</h2>
          <span>
            {data && data.total >= 0
              ? `${data.total} registros`
              : data?.items.length
              ? `${data.items.length} registros (página ${page})`
              : '0 registros'}
          </span>
        </div>
        {error && <p className="error">{error}</p>}
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>CNPJ14</th>
                <th>Razão Social</th>
                <th>Nome Fantasia</th>
                <th>Situação</th>
                <th>Data Situação</th>
                <th>UF</th>
                <th>Município</th>
                <th>Bairro</th>
                <th>CNAE</th>
                <th>Telefone 1</th>
                <th>Telefone 2</th>
                <th>Fax</th>
                <th>Email</th>
                <th>Ações</th>
              </tr>
            </thead>
            <tbody>
              {data?.items.map((item: any, index: number) => (
                <tr key={index}>
                  <td>{item.cnpj14}</td>
                  <td>{item.razao_social || '-'}</td>
                  <td>{item.nome_fantasia || '-'}</td>
                  <td>{item.situacao_cadastral}</td>
                  <td>{item.data_situacao_cadastral || '-'}</td>
                  <td>{item.uf}</td>
                  <td>{item.municipio}</td>
                  <td>{item.bairro || '-'}</td>
                  <td>{item.cnae_fiscal_principal}</td>
                  <td>{item.ddd1 && item.telefone1 ? `(${item.ddd1}) ${item.telefone1}` : '-'}</td>
                  <td>{item.ddd2 && item.telefone2 ? `(${item.ddd2}) ${item.telefone2}` : '-'}</td>
                  <td>{item.ddd_fax && item.fax ? `(${item.ddd_fax}) ${item.fax}` : '-'}</td>
                  <td>{item.email || '-'}</td>
                  <td>
                    <button
                      className="action-button"
                      onClick={() => handleCnpjClick(String(item.cnpj_basico))}
                    >
                      Ver Sócios
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="pagination">
          <button
            disabled={page === 1 || loading}
            onClick={() => {
              setPage((prev) => Math.max(1, prev - 1));
              // Trigger search with new page
              setTimeout(fetchData, 0);
            }}
          >
            Anterior
          </button>
          <span>Página {page}</span>
          <button
            disabled={loading || !data || (data.has_more === false)}
            onClick={() => {
              setPage((prev) => prev + 1);
              // Trigger search with new page
              setTimeout(fetchData, 0);
            }}
          >
            Próxima
          </button>
        </div>
      </section>

      {/* Modal de Municípios */}
      {showMunicipioModal && (
        <div className="modal-overlay" onClick={() => setShowMunicipioModal(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Buscar Município</h2>
              <button onClick={() => setShowMunicipioModal(false)}>✕</button>
            </div>
            <div className="search-box">
              <input
                placeholder="Digite o nome do município"
                value={municipioSearch}
                onChange={(e) => setMunicipioSearch(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && searchMunicipios()}
              />
              <button onClick={searchMunicipios} disabled={loadingMunicipios}>
                {loadingMunicipios ? 'Buscando...' : 'Buscar'}
              </button>
            </div>
            <div className="search-results">
              {municipios.map((mun) => (
                <div
                  key={mun.codigo}
                  className="search-result-item"
                  onClick={() => {
                    setSelectedMunicipio(mun);
                    setShowMunicipioModal(false);
                  }}
                >
                  <strong>{mun.descricao}</strong>
                  <span>Código: {mun.codigo}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Modal de CNAEs */}
      {showCnaeModal && (
        <div className="modal-overlay" onClick={() => setShowCnaeModal(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Buscar CNAEs</h2>
              <button onClick={() => setShowCnaeModal(false)}>✕</button>
            </div>
            <div className="search-box">
              <input
                placeholder="Digite a descrição do CNAE"
                value={cnaeSearch}
                onChange={(e) => setCnaeSearch(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && searchCnaes()}
              />
              <button onClick={searchCnaes} disabled={loadingCnaes}>
                {loadingCnaes ? 'Buscando...' : 'Buscar'}
              </button>
            </div>
            {selectedCnaes.length > 0 && (
              <div className="selected-items">
                <strong>Selecionados:</strong>
                {selectedCnaes.map((cnae) => (
                  <span key={cnae.codigo} className="selected-tag">
                    {cnae.codigo} - {cnae.descricao}
                    <button onClick={() => setSelectedCnaes(selectedCnaes.filter(c => c.codigo !== cnae.codigo))}>✕</button>
                  </span>
                ))}
              </div>
            )}
            <div className="search-results">
              {cnaes.map((cnae) => (
                <div
                  key={cnae.codigo}
                  className="search-result-item"
                  onClick={() => {
                    if (!selectedCnaes.find(c => c.codigo === cnae.codigo)) {
                      setSelectedCnaes([...selectedCnaes, cnae]);
                    }
                  }}
                >
                  <strong>{cnae.codigo}</strong>
                  <span>{cnae.descricao}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Modal de Sócios */}
      {selectedCnpj && (
        <div className="modal-overlay" onClick={closeModal}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Sócios do CNPJ {selectedCnpj}</h2>
              <button onClick={closeModal}>✕</button>
            </div>
            {loadingModal ? (
              <p>Carregando...</p>
            ) : sociosData ? (
              <div>
                {sociosData.items.length > 0 && (
                  <p>{sociosData.items.length} sócio(s) encontrado(s)</p>
                )}
                <table>
                  <thead>
                    <tr>
                      <th>Nome</th>
                      <th>CPF/CNPJ</th>
                      <th>Qualificação</th>
                      <th>Ações</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sociosData.items.map((socio, idx) => (
                      <tr key={idx}>
                        <td>{socio.nome_socio || '-'}</td>
                        <td>{socio.cnpj_cpf_socio || '-'}</td>
                        <td>{socio.codigo_qualificacao_socio || '-'}</td>
                        <td>
                          <button
                            className="action-button"
                            onClick={() => handleSocioClick(String(socio.cnpj_cpf_socio))}
                          >
                            Ver Empresas
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}
          </div>
        </div>
      )}

      {/* Modal de Empresas do Sócio */}
      {selectedSocio && (
        <div className="modal-overlay" onClick={closeModal}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Empresas do Sócio {selectedSocio}</h2>
              <button onClick={closeModal}>✕</button>
            </div>
            {loadingModal ? (
              <p>Carregando...</p>
            ) : socioEmpresasData ? (
              <div>
                {socioEmpresasData.items.length > 0 && (
                  <p>{socioEmpresasData.items.length} empresa(s) encontrada(s)</p>
                )}
                <table>
                  <thead>
                    <tr>
                      <th>CNPJ Básico</th>
                      <th>Razão Social</th>
                      <th>Qualificação</th>
                      <th>Ações</th>
                    </tr>
                  </thead>
                  <tbody>
                    {socioEmpresasData.items.map((item: any, idx: number) => (
                      <tr key={idx}>
                        <td>{item.cnpj_basico}</td>
                        <td>{item.razao_social || '-'}</td>
                        <td>{item.codigo_qualificacao_socio || '-'}</td>
                        <td>
                          <button
                            className="action-button"
                            onClick={() => handleCnpjClick(String(item.cnpj_basico))}
                          >
                            Ver Sócios
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
