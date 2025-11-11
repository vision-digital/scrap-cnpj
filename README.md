# Projeto Scrap CNPJ

Pipeline completo para baixar, processar e expor os dados públicos do CNPJ utilizando FastAPI, PostgreSQL e um frontend React.

## Arquitetura

- **Backend (FastAPI/Python)**: orquestra download, extração, carga (COPY no PostgreSQL) e disponibiliza APIs de busca/exportação.
- **PostgreSQL 16**: armazena dezenas de milhões de registros com índices BTREE e GIN (pg_trgm) para consultas rápidas.
- **Frontend (React + Vite)**: interface para filtros, exportação em CSV e acompanhamento de versão/atualização.
- **Diretório `./data`**: guarda downloads/extrajções para reaproveitamento e auditoria.

## Como executar

1. Crie o arquivo `.env` a partir de `.env.example` (credenciais do Postgres).
2. Suba os serviços:
   ```bash
   docker compose up --build
   ```
3. Frontend: `http://localhost:5173`. API: `http://localhost:8000`.

## Pipeline de atualização

1. **Descoberta de release** – lê o índice oficial e escolhe a pasta mais recente (ou a informada).
2. **Download** – todos os `.zip` são armazenados em `data/raw/<release>` (reaproveitados quando já existirem).
3. **Extração** – os arquivos são descompactados em `data/staging/<release>` (também reutilizados se presentes).
4. **Carga** – o Loader percorre `EMPRECSV`, `ESTABELE`, `SOCIOCSV`, `SIMECSV` e usa `COPY ... FROM STDIN` em lotes de 50k linhas. O `tqdm` mostra progresso de cada arquivo e os logs sinalizam início/fim/erros.
5. **Versionamento** – `data_versions` guarda release, status (`running/completed/failed`), timestamps e notas. Exposto por `/api/version/latest` e `/api/stats`.
6. **Limpeza opcional** – `raw` e `staging` permanecem por padrão; ajuste `SCRAP_CLEANUP_*` no `.env` para exclusão automática.

### Execução manual

```bash
cd backend
python -m app.tasks.update_data --release 2025-11  # opcional
```

### Via frontend/API

- Botão “Verificar novidades” → `POST /api/updates/run`.
- Painel consome `GET /api/updates/status` e `GET /api/stats` para progresso.

## Esquema (PostgreSQL)

| Tabela             | Campos-chave / Índices                                        |
|--------------------|----------------------------------------------------------------|
| `empresas`         | `cnpj_basico` PK, BTREE para natureza/porte e GIN (pg_trgm) em razão social. |
| `estabelecimentos` | `cnpj14` PK, BTREE para UF/município/CNAE, GIN trigram em nome fantasia. |
| `socios`           | PK sintético, BTREE para `cnpj_basico` e documento, trigram em nome. |
| `simples`          | `cnpj_basico` PK, BTREE para `opcao_simples/opcao_mei`. |
| `data_versions`    | histórico das execuções. |

As extensões `pg_trgm` e `btree_gin` são criadas automaticamente durante o startup.

## APIs principais

- `GET /api/health`
- `GET /api/version/latest`
- `GET /api/updates/status`
- `POST /api/updates/run`
- `GET /api/search/<dataset>` (`empresas`, `estabelecimentos`, `socios`, `simples`)
- `GET /api/export/<dataset>` – CSV
- `GET /api/stats` – contagem total + release atual

## Logs e acompanhamento

- `docker compose logs backend -f` – mostra cada etapa (download → extração → COPY) com contagens e barras de progresso.
- `docker compose logs postgres -f` – monitora checkpoints/WAL.
- `GET /api/stats` – verifica crescimento das tabelas sem rodar consultas pesadas manualmente.

## Próximos passos sugeridos

1. Ajustar parâmetros do PostgreSQL conforme hardware (shared_buffers, work_mem, max_wal_size...).
2. Adicionar autenticação nos endpoints sensíveis (`/api/updates/run`, `/api/export`).
3. Incluir os arquivos auxiliares (CNAE, municípios etc.) para enriquecer filtros.
4. Integrar observabilidade (Prometheus + Grafana ou pg_stat_statements) para acompanhar ingestões/consultas.
