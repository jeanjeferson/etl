"""
Extrator de Dados de Volume Simplificado
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import Optional, Dict, List
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from pathlib import Path
import yaml
from glob import glob

load_dotenv()

class SQLQuery:
    """Extrator simplificado para consultar banco de dados e extrair dados de SQL."""

    def __init__(self, config_file: str = "config/databases.yaml"):
        """Inicializa conexão com banco de dados."""
        self.verbose = False
        self.config = self._load_config(config_file)
        self.engine = self._create_engine()
        self._ensure_dataset_dir()

    def _load_volume_query(self) -> str:
        """Carrega query de volume do arquivo SQL."""
        sql_file = Path("sql/volume.sql")
        
        if not sql_file.exists():
            if self.verbose:
                print("⚠️ Arquivo sql/volume.sql não encontrado")
            return ""
        
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                query = f.read().strip()
            if self.verbose:
                print(f"✅ Query de volume carregada de sql/volume.sql")
            return query
        except Exception as e:
            if self.verbose:
                print(f"❌ Erro ao carregar sql/volume.sql: {e}")
            return ""

    def _load_config(self, config_file: str) -> dict:
        """Carrega configurações do arquivo YAML."""
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            
            # Carregar apenas a query de volume
            volume_query = self._load_volume_query()
            config["volume_query"] = volume_query
            return config
        except Exception as e:
            print(f"❌ Erro ao carregar configurações: {e}")
            raise

    def _create_engine(self):
        """Cria engine SQLAlchemy com configurações do .env."""
        try:
            start = time.perf_counter()
            # Configurações do banco
            driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')
            server = os.getenv('DB_SERVER', 'localhost')
            port = os.getenv('DB_PORT', '1433')
            username = os.getenv('DB_UID')
            password = os.getenv('DB_PWD')
            
            # String de conexão ODBC
            odbc_conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server},{port};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
            )
            
            # Criar engine SQLAlchemy
            quoted_conn_str = quote_plus(odbc_conn_str)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}")
            
            elapsed = time.perf_counter() - start
            if self.verbose:
                print(f"✅ Conexão com banco de dados estabelecida (⏱️ {elapsed:.2f}s)")
            return engine
            
        except Exception as e:
            print(f"❌ Erro na conexão: {e}")
            return None

    def _ensure_dataset_dir(self):
        """Garante que a pasta dataset existe."""
        os.makedirs("data", exist_ok=True)

    def _execute_query(self, database: str, query: str, params: dict = None) -> pd.DataFrame:
        """Executa query em um database específico."""
        if not self.engine:
            print("❌ Engine não disponível")
            return pd.DataFrame()
        
        try:
            start_total = time.perf_counter()
            # Modificar conexão para usar database específico
            connection_str = str(self.engine.url)
            if "DATABASE=" not in connection_str:
                # Adicionar database à string de conexão
                odbc_part = connection_str.split("odbc_connect=")[1]
                new_odbc = f"{odbc_part.rstrip('%3B')};DATABASE={database}"
                connection_str = connection_str.split("odbc_connect=")[0] + f"odbc_connect={quote_plus(new_odbc)}"
                
                temp_engine = create_engine(connection_str)
            else:
                temp_engine = self.engine
            
            # Executar query
            with temp_engine.connect() as conn:
                start_query = time.perf_counter()
                df = pd.read_sql(text(query), conn, params=params)
                query_elapsed = time.perf_counter() - start_query
            
            total_elapsed = time.perf_counter() - start_total
            if self.verbose:
                print(f"🗄️ Query no DB '{database}' retornou {len(df):,} linhas (execução: {query_elapsed:.2f}s, total: {total_elapsed:.2f}s)")
            return df
            
        except Exception as e:
            print(f"❌ Erro executando query no {database}: {e}")
            return pd.DataFrame()
    
    def _load_sql_files(self, sql_dir: str = "sql") -> Dict[str, str]:
        """
        Carrega todos os arquivos SQL de um diretório.
        
        Args:
            sql_dir: Diretório contendo os arquivos SQL
            
        Returns:
            Dicionário {nome_arquivo: conteúdo_query}
        """
        sql_files = {}
        sql_path = Path(sql_dir)
        
        if not sql_path.exists():
            print(f"❌ Diretório {sql_dir} não encontrado")
            return sql_files
        
        # Buscar todos os arquivos .sql
        for sql_file in sql_path.glob("*.sql"):
            try:
                with open(sql_file, "r", encoding="utf-8") as f:
                    query = f.read().strip()
                
                # Nome do arquivo sem extensão
                file_name = sql_file.stem
                sql_files[file_name] = query
                
                if self.verbose:
                    print(f"📄 Carregado: {sql_file.name}")
                    
            except Exception as e:
                print(f"⚠️ Erro ao carregar {sql_file.name}: {e}")
        
        return sql_files
    
    def execute_all_queries(self, output_base_dir: str = "data") -> Dict[str, any]:
        """
        Executa todas as queries SQL contra todos os databases configurados.
        Salva os resultados como arquivos parquet em data/{database}/ folders.
        
        Args:
            output_base_dir: Diretório base para salvar os arquivos (padrão: 'data')
            
        Returns:
            Dicionário com estatísticas de execução e erros
        """
        print("=" * 80)
        print("🚀 Iniciando execução de queries em múltiplos databases")
        print("=" * 80)
        
        # Carregar arquivos SQL
        sql_files = self._load_sql_files()
        if not sql_files:
            print("❌ Nenhum arquivo SQL encontrado na pasta sql/")
            return {"success": False, "error": "No SQL files found"}
        
        print(f"\n📊 Total de queries a executar: {len(sql_files)}")
        print(f"   Arquivos: {', '.join(sql_files.keys())}")
        
        # Obter lista de databases
        databases = self.config.get('databases', [])
        if not databases:
            print("❌ Nenhum database configurado")
            return {"success": False, "error": "No databases configured"}
        
        print(f"\n🗄️  Total de databases: {len(databases)}")
        print(f"   Databases: {', '.join(databases)}")
        
        # Estatísticas
        stats = {
            "total_executions": 0,
            "successful": 0,
            "failed": 0,
            "errors": [],
            "details": []
        }
        
        start_time = time.perf_counter()
        
        # Iterar sobre cada database
        for db_index, database in enumerate(databases, 1):
            print(f"\n{'=' * 80}")
            print(f"📊 Database [{db_index}/{len(databases)}]: {database}")
            print(f"{'=' * 80}")
            
            # Criar diretório para o database
            db_output_dir = Path(output_base_dir) / database
            db_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Iterar sobre cada query SQL
            for query_index, (query_name, query_content) in enumerate(sql_files.items(), 1):
                stats["total_executions"] += 1
                
                print(f"\n  [{query_index}/{len(sql_files)}] Executando: {query_name}.sql")
                
                try:
                    # Executar query
                    query_start = time.perf_counter()
                    df = self._execute_query(database, query_content)
                    query_elapsed = time.perf_counter() - query_start
                    
                    if df.empty:
                        print(f"  ⚠️  Query retornou 0 linhas - salvando arquivo vazio")
                    
                    # Salvar como parquet
                    output_file = db_output_dir / f"{query_name}.parquet"
                    df.to_parquet(output_file, index=False, engine='pyarrow')
                    
                    file_size = output_file.stat().st_size / 1024  # KB
                    
                    print(f"  ✅ Salvo: {output_file}")
                    print(f"     📈 Linhas: {len(df):,} | Colunas: {len(df.columns)} | Tamanho: {file_size:.1f} KB | Tempo: {query_elapsed:.2f}s")
                    
                    stats["successful"] += 1
                    stats["details"].append({
                        "database": database,
                        "query": query_name,
                        "rows": len(df),
                        "cols": len(df.columns),
                        "time": query_elapsed,
                        "status": "success"
                    })
                    
                except Exception as e:
                    error_msg = f"Database: {database}, Query: {query_name}.sql, Error: {str(e)}"
                    print(f"  ❌ Erro: {str(e)}")
                    
                    stats["failed"] += 1
                    stats["errors"].append(error_msg)
                    stats["details"].append({
                        "database": database,
                        "query": query_name,
                        "status": "failed",
                        "error": str(e)
                    })
        
        total_elapsed = time.perf_counter() - start_time
        
        # Sumário final
        print(f"\n{'=' * 80}")
        print("📊 SUMÁRIO DA EXECUÇÃO")
        print(f"{'=' * 80}")
        print(f"✅ Sucesso: {stats['successful']}/{stats['total_executions']}")
        print(f"❌ Falhas: {stats['failed']}/{stats['total_executions']}")
        print(f"⏱️  Tempo total: {total_elapsed:.2f}s")
        print(f"📁 Diretório de saída: {Path(output_base_dir).absolute()}")
        
        if stats['errors']:
            print(f"\n⚠️  Erros encontrados:")
            for error in stats['errors']:
                print(f"   - {error}")
        
        print(f"\n{'=' * 80}")
        
        stats["total_time"] = total_elapsed
        stats["success"] = True
        
        return stats
    
    def execute_queries_for_database(self, database: str, output_dir: str = "data") -> Dict[str, any]:
        """
        Executa todas as queries SQL para um database específico.
        Salva os resultados como arquivos parquet em data/{database}/ folder.
        
        Args:
            database: Nome do database para executar as queries
            output_dir: Diretório base para salvar os arquivos (padrão: 'data')
            
        Returns:
            Dicionário com estatísticas de execução e erros
        """
        print("=" * 80)
        print(f"🚀 Iniciando execução de queries no database: {database}")
        print("=" * 80)
        
        # Validar se database existe na configuração
        databases = self.config.get('databases', [])
        if database not in databases:
            print(f"❌ Database '{database}' não encontrado na configuração")
            print(f"   Databases disponíveis: {', '.join(databases)}")
            return {
                "success": False, 
                "error": f"Database '{database}' not configured",
                "available_databases": databases
            }
        
        # Carregar arquivos SQL
        sql_files = self._load_sql_files()
        if not sql_files:
            print("❌ Nenhum arquivo SQL encontrado na pasta sql/")
            return {"success": False, "error": "No SQL files found"}
        
        print(f"\n📊 Total de queries a executar: {len(sql_files)}")
        print(f"   Arquivos: {', '.join(sql_files.keys())}")
        print(f"🗄️  Database: {database}")
        
        # Criar diretório para o database
        db_output_dir = Path(output_dir) / database
        db_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Estatísticas
        stats = {
            "database": database,
            "total_executions": 0,
            "successful": 0,
            "failed": 0,
            "errors": [],
            "details": []
        }
        
        start_time = time.perf_counter()
        
        # Iterar sobre cada query SQL
        for query_index, (query_name, query_content) in enumerate(sql_files.items(), 1):
            stats["total_executions"] += 1
            
            print(f"\n[{query_index}/{len(sql_files)}] Executando: {query_name}.sql")
            
            try:
                # Executar query
                query_start = time.perf_counter()
                df = self._execute_query(database, query_content)
                query_elapsed = time.perf_counter() - query_start
                
                if df.empty:
                    print(f"  ⚠️  Query retornou 0 linhas - salvando arquivo vazio")
                
                # Salvar como parquet
                output_file = db_output_dir / f"{query_name}.parquet"
                df.to_parquet(output_file, index=False, engine='pyarrow')
                
                file_size = output_file.stat().st_size / 1024  # KB
                
                print(f"  ✅ Salvo: {output_file}")
                print(f"     📈 Linhas: {len(df):,} | Colunas: {len(df.columns)} | Tamanho: {file_size:.1f} KB | Tempo: {query_elapsed:.2f}s")
                
                stats["successful"] += 1
                stats["details"].append({
                    "query": query_name,
                    "rows": len(df),
                    "cols": len(df.columns),
                    "time": query_elapsed,
                    "status": "success"
                })
                
            except Exception as e:
                error_msg = f"Query: {query_name}.sql, Error: {str(e)}"
                print(f"  ❌ Erro: {str(e)}")
                
                stats["failed"] += 1
                stats["errors"].append(error_msg)
                stats["details"].append({
                    "query": query_name,
                    "status": "failed",
                    "error": str(e)
                })
        
        total_elapsed = time.perf_counter() - start_time
        
        # Sumário final
        print(f"\n{'=' * 80}")
        print("📊 SUMÁRIO DA EXECUÇÃO")
        print(f"{'=' * 80}")
        print(f"🗄️  Database: {database}")
        print(f"✅ Sucesso: {stats['successful']}/{stats['total_executions']}")
        print(f"❌ Falhas: {stats['failed']}/{stats['total_executions']}")
        print(f"⏱️  Tempo total: {total_elapsed:.2f}s")
        print(f"📁 Diretório de saída: {db_output_dir.absolute()}")
        
        if stats['errors']:
            print(f"\n⚠️  Erros encontrados:")
            for error in stats['errors']:
                print(f"   - {error}")
        
        print(f"\n{'=' * 80}")
        
        stats["total_time"] = total_elapsed
        stats["success"] = True
        
        return stats
    
if __name__ == '__main__':
    # Exemplo de uso: executar todas as queries em todos os databases
    extractor = SQLQuery()
    
    # Habilitar modo verbose para mais informações
    extractor.verbose = True
    
    # Executar todas as queries e salvar como parquet
    results = extractor.execute_all_queries(output_base_dir="data")
    
    print("\n✅ Execução concluída!")
    print(f"Total de execuções bem-sucedidas: {results.get('successful', 0)}")
    print(f"Total de falhas: {results.get('failed', 0)}")
    
