# ETL Pipeline - Extração SQL e Upload FTP

Sistema ETL para extração de dados de SQL Server e upload automático para FTP/SFTP.

## 📋 Requisitos

- Python >= 3.12
- ODBC Driver for SQL Server instalado
- Acesso ao banco de dados SQL Server
- Acesso ao servidor FTP/SFTP

## 🚀 Instalação

### Opção 1: Docker (Recomendado)

#### 1. Configurar credenciais

Copie o arquivo `env.example` para `.env` e preencha com suas credenciais:

```bash
# Linux/Mac
cp env.example .env

# Windows
copy env.example .env
```

Edite o arquivo `.env`:

```env
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_SERVER=seu-servidor.database.windows.net
DB_PORT=1433
DB_UID=seu_usuario
DB_PWD=sua_senha
```

#### 2. Build e executar com Docker Compose

```bash
# Build da imagem
docker-compose build

# Iniciar o serviço
docker-compose up -d

# Ver logs
docker-compose logs -f

# Parar o serviço
docker-compose down
```

A API estará disponível em: http://localhost:8000

#### 3. Build e executar com Docker (sem compose)

```bash
# Build da imagem
docker build -t etl-pipeline-api .

# Executar container
docker run -d \
  --name etl-api \
  -p 8000:8000 \
  --env-file .env \
  -v $(pwd)/data:/app/data \
  etl-pipeline-api

# Ver logs
docker logs -f etl-api

# Parar container
docker stop etl-api
docker rm etl-api
```

### Opção 2: Instalação Local

#### 1. Criar ambiente virtual e instalar dependências

```bash
# Criar ambiente virtual
uv venv

# Ativar ambiente (Windows)
.venv\Scripts\activate

# Instalar dependências
uv pip install -e .
```

#### 3. Verificar driver ODBC instalado

Para verificar quais drivers ODBC estão instalados no Windows:

```powershell
Get-OdbcDriver | Select-Object -Property Name
```

Drivers comuns:
- `ODBC Driver 18 for SQL Server` (mais recente)
- `ODBC Driver 17 for SQL Server`
- `SQL Server Native Client 11.0`

## 📁 Estrutura do Projeto

```
etl/
├── config/
│   └── databases.yaml          # Lista de databases a processar
├── sql/                         # Arquivos SQL com queries
│   ├── clientes.sql
│   ├── consultor.sql
│   ├── estoque.sql
│   ├── lojas.sql
│   ├── metas_emp.sql
│   ├── meta_fun.sql
│   ├── produtos.sql
│   └── vendas.sql
├── utils/
│   ├── sql_query.py            # Classe para executar queries
│   └── ftp_uploader.py         # Classe para upload FTP
├── data/                        # Saída dos arquivos parquet (gerado)
├── api.py                       # FastAPI application (execução assíncrona)
├── run_sql.py                   # Script principal (execução direta)
├── pyproject.toml              # Dependências do projeto
├── Dockerfile                   # Configuração Docker
├── docker-compose.yml          # Orquestração Docker
├── .dockerignore               # Arquivos excluídos do Docker build
├── .gitignore                  # Arquivos excluídos do Git
├── env.example                 # Exemplo de variáveis de ambiente
└── .env                         # Credenciais (criar a partir do env.example)
```

## 🎯 Uso

### Opção 1: FastAPI (Recomendado para execução assíncrona)

Inicie o servidor da API:

```bash
uvicorn api:app --reload
```

A API estará disponível em `http://localhost:8000`

**Endpoints disponíveis:**

- `GET /` - Informações da API
- `GET /health` - Health check
- `POST /run-pipeline` - Inicia pipeline ETL em background
- `GET /jobs/{job_id}` - Consulta status de um job específico
- `GET /jobs` - Lista todos os jobs
- `GET /docs` - Documentação interativa (Swagger UI)

**Exemplo de uso:**

```bash
# Iniciar pipeline
curl -X POST "http://localhost:8000/run-pipeline?output_dir=data&forecast_type=data&verbose=true"
# Retorna: {"job_id": "uuid", "status": "pending", ...}

# Consultar status
curl "http://localhost:8000/jobs/{job_id}"

# Listar todos os jobs
curl "http://localhost:8000/jobs"
```

### Opção 2: Script direto (Execução síncrona)

```bash
python run_sql.py
```

Isso irá:
1. 📊 Executar todas as queries SQL dos arquivos em `sql/`
2. 💾 Salvar resultados em `data/{database}/*.parquet`
3. 📤 Fazer upload automático para FTP em `ai/{database}/data/`
4. 📋 Exibir resumo completo da execução

### Executar apenas extração SQL

```python
from utils.sql_query import SQLQuery

extractor = SQLQuery()
extractor.verbose = True
results = extractor.execute_all_queries(output_base_dir="data")
```

### Executar apenas upload FTP

```python
from utils.ftp_uploader import ForecastFTPUploader
from pathlib import Path

ftp = ForecastFTPUploader()
if ftp._connect():
    db_path = Path('data/005ATS_ERP_BI')
    parquet_files = [str(f) for f in db_path.glob('*.parquet')]
    
    result = ftp.upload_data(
        database_name='005ATS_ERP_BI',
        forecast_type='data',
        file_paths=parquet_files
    )
    ftp.disconnect()
```

## ⚙️ Configuração

### Databases (`config/databases.yaml`)

Lista os databases que serão processados:

```yaml
databases:
  - '005ATS_ERP_BI'
  - '005NO_ERP_BI'
  - '005RG_ERP_BI'
  # ...
```

### Queries SQL (`sql/*.sql`)

Adicione arquivos `.sql` na pasta `sql/`. Cada arquivo será:
- Executado em todos os databases configurados
- Salvo como `data/{database}/{nome_arquivo}.parquet`
- Enviado para `ai/{database}/data/{nome_arquivo}.parquet` no FTP

### Credenciais FTP

As credenciais FTP estão hardcoded em `utils/ftp_uploader.py`. 
Para ambientes de produção, considere movê-las para variáveis de ambiente.

## 📊 Saída

### Estrutura de arquivos locais

```
data/
├── 005ATS_ERP_BI/
│   ├── clientes.parquet
│   ├── consultor.parquet
│   ├── estoque.parquet
│   └── ...
├── 005NO_ERP_BI/
│   └── ...
└── ...
```

### Estrutura no FTP

```
ai/
├── 005ATS_ERP_BI/
│   └── data/
│       ├── clientes.parquet
│       ├── consultor.parquet
│       └── ...
└── ...
```

## 🔧 Troubleshooting

### Erro: "Nome da fonte de dados não encontrado"

```
pyodbc.InterfaceError: ('IM002', '[IM002] [Microsoft][ODBC Driver Manager] 
Nome da fonte de dados não encontrado...')
```

**Soluções:**
1. Verifique se o arquivo `.env` existe e está configurado
2. Verifique se o driver ODBC especificado está instalado
3. Teste a conexão manualmente

### Erro: "FTP connection failed"

**Soluções:**
1. Verifique se o servidor FTP está acessível
2. Verifique credenciais em `utils/ftp_uploader.py`
3. Verifique firewall/rede

### Arquivos parquet vazios

Se as queries retornam 0 linhas:
1. Verifique se as tabelas existem no database
2. Verifique permissões do usuário do banco de dados
3. Teste as queries manualmente

## 📦 Dependências Principais

- **pandas**: Manipulação de dados
- **sqlalchemy**: Conexão com SQL Server
- **pyodbc**: Driver ODBC
- **pyarrow**: Suporte a arquivos Parquet
- **pyyaml**: Leitura de configs YAML
- **python-dotenv**: Variáveis de ambiente
- **paramiko**: Conexão FTP/SFTP
- **fastapi**: Framework web para APIs
- **uvicorn**: Servidor ASGI para FastAPI

## 🐳 Docker

### Arquivos Docker

- **Dockerfile**: Imagem base com Python 3.12 e ODBC Driver 18
- **docker-compose.yml**: Orquestração com volumes e variáveis de ambiente
- **.dockerignore**: Arquivos excluídos do build

### Volumes

O docker-compose.yml configura os seguintes volumes:

- `./data:/app/data` - Dados parquet gerados
- `./logs:/app/logs` - Logs da aplicação
- `./config:/app/config` - Configurações (read-only)
- `./sql:/app/sql` - Queries SQL (read-only)

### Variáveis de Ambiente

Configure no arquivo `.env`:

```env
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_SERVER=seu-servidor.database.windows.net
DB_PORT=1433
DB_UID=seu_usuario
DB_PWD=sua_senha
TZ=America/Sao_Paulo
```

### Health Check

O container inclui health check automático:
- Intervalo: 30s
- Timeout: 10s
- Retries: 3
- Start period: 40s
- Endpoint: `http://localhost:8000/health`

## 📝 Licença

Este projeto é de uso interno.

