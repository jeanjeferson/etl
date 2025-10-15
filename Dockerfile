FROM python:3.12-slim

WORKDIR /app

# Variáveis de ambiente padrões
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=America/Sao_Paulo

# Dependências de sistema para Python packages e SQL Server
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    netcat-openbsd \
    unixodbc \
    unixodbc-dev \
    gnupg2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instalar Microsoft ODBC Driver 18 para SQL Server (Debian 12)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia dependências primeiro (melhor uso do cache do Docker)
COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pandas>=2.3.3 \
        python-dotenv>=1.1.1 \
        sqlalchemy>=2.0.44 \
        pyyaml>=6.0.3 \
        pyarrow>=21.0.0 \
        pyodbc>=5.2.0 \
        paramiko>=4.0.0 \
        fastapi>=0.115.0 \
        uvicorn[standard]>=0.32.0 \
        requests>=2.31.0 \
        supabase>=2.22.0

# Copia arquivos principais da aplicação
COPY api.py ./
COPY run_sql.py ./

# Copia diretórios necessários
COPY utils ./utils
COPY config ./config
COPY sql ./sql

# Cria diretórios necessários para runtime
RUN mkdir -p ./data ./logs \
    && chmod -R 755 /app

# Expor porta da API
EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=40s \
    CMD curl --fail http://localhost:8000/health || exit 1

# Run FastAPI server com Uvicorn
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]

