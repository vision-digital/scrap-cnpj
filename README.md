# Projeto Scrap CNPJ

Pipeline completo para baixar, limpar, carregar e expor os dados públicos do CNPJ da Receita Federal.

## Arquitetura

- **Backend (FastAPI/Python)**: orquestra downloads, extração, carga MySQL e expõe APIs de busca/exportação.
- **MySQL 8**: armazenamento persistente dos datasets (Empresas, Estabelecimentos, Sócios, Simples e versões).
- **Frontend (React + Vite)**: interface para filtros, exportação e gatilho de atualização.
- **Volumes**: ./data guarda temporários e staging até limpeza automática do pipeline.

## Como executar

1. Crie o arquivo .env baseado em .env.example.
2. Inicialize os serviços com Docker Compose:
   `ash
   docker-compose up --build
   `
3. Acesse http://localhost:5173 para o frontend. A API estará em http://localhost:8000.

## Pipeline de atualização

O pipeline implementa as etapas solicitadas:

1. **Descoberta da versão**: ReceitaFederalClient identifica a pasta mais recente no índice oficial.
2. **Download**: DownloadManager baixa todos os .zip da release escolhida para data/raw/<release>.
3. **Extração**: Extractor descompacta (inclusive pacotes particionados) para data/staging/<release>.
4. **Carga**: Loader lê os .CSV/.TXT em fluxo, aplica mapeamentos e insere em lotes (atch_size configurável) nas tabelas MySQL.
5. **Versionamento**: DataVersion armazena release, status e timestamps para auditoria.
6. **Limpeza**: diretórios aw e staging da release são removidos ao final para liberar disco.

### Execução manual

Use o CLI embutido:
`ash
cd backend
python -m app.tasks.update_data --release 2025-11  # opcional
`
Sem --release, o pipeline busca automaticamente a versão mais recente.

### Via frontend/API

- Botão "Verificar novidades" chama POST /api/updates/run e dispara o pipeline em background.
- GET /api/updates/status informa release atual e progresso.

## Estrutura do banco

| Tabela             | Chave primária      | Observações |
|--------------------|---------------------|-------------|
| data_versions    | id                | Histórico da release carregada. |
| empresas         | cnpj_basico       | Dados corporativos básicos. |
| estabelecimentos | cnpj (14 dígitos) | Inclui endereço, CNAE, situação e contatos. |
| socios           | id autoincrement  | Sócios e representantes. |
| simples          | cnpj_basico       | Situação no Simples/MEI. |

Os mapeamentos respeitam o layout oficial do CNPJ e podem ser estendidos para CNAEs, Municípios, Motivos etc.

## APIs principais

- GET /api/health – status da API.
- GET /api/version/latest – release atual.
- POST /api/updates/run – dispara atualização (payload opcional { "release": "YYYY-MM" }).
- GET /api/search/<dataset> – filtros e paginação para empresas, estabelecimentos, socios, simples.
- GET /api/export/<dataset> – exporta CSV respeitando filtros.

## Frontend

- Seleção de dataset e filtros contextuais.
- Tabela responsiva com paginação.
- Exportação em CSV com o mesmo conjunto de filtros.
- Painel para acompanhar a versão carregada e disparar novas coletas.

## Próximos passos sugeridos

- Adicionar autenticação/controle de acesso para o gatilho de atualização.
- Implementar cache ou views materializadas para acelerar buscas complexas.
- Expandir suporte a demais arquivos auxiliares (CNAEs, municípios, motivos, qualificações) para filtros mais ricos.
- Configurar workers separados (ex.: Celery/RQ) para tarefas de atualização e monitoramento.
