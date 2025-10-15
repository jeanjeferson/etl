"""
ETL Pipeline Runner
Executa queries SQL e faz upload dos resultados para FTP/SFTP
"""

from utils.sql_query import SQLQuery
from utils.ftp_uploader import ForecastFTPUploader
from utils.upload_supabase import SupabaseUploader
from pathlib import Path
from typing import Dict, Any, List, Optional
import time
import tempfile
import shutil


def run_etl_pipeline(output_dir: str = "data", verbose: bool = True) -> Dict[str, Any]:
    """
    Executa pipeline de extração de dados SQL.
    
    Args:
        output_dir: Diretório base para salvar os arquivos parquet
        verbose: Exibir logs detalhados
        
    Returns:
        Dicionário com estatísticas de execução
    """
    print("=" * 80)
    print("📊 FASE 1: EXTRAÇÃO DE DADOS SQL")
    print("=" * 80)
    
    try:
        # Instanciar SQLQuery
        extractor = SQLQuery()
        extractor.verbose = verbose
        
        # Executar todas as queries
        results = extractor.execute_all_queries(output_base_dir=output_dir)
        
        return results
        
    except Exception as e:
        print(f"\n❌ Erro na extração de dados: {e}")
        return {
            'success': False,
            'error': str(e),
            'successful': 0,
            'failed': 0,
            'total_executions': 0
        }


def upload_to_ftp(data_dir: str = "data", forecast_type: str = "data") -> Dict[str, Any]:
    """
    Faz upload dos arquivos parquet gerados para FTP/SFTP.
    
    Args:
        data_dir: Diretório base contendo as pastas de databases
        forecast_type: Tipo de dados ('data', 'vendas', 'volume', etc)
        
    Returns:
        Dicionário com estatísticas de upload
    """
    print("\n" + "=" * 80)
    print("📤 FASE 2: UPLOAD PARA FTP/SFTP")
    print("=" * 80)
    
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"❌ Diretório {data_dir} não encontrado")
        return {
            'success': False,
            'error': f'Directory {data_dir} not found',
            'total_uploads': 0,
            'successful_uploads': 0,
            'failed_uploads': 0
        }
    
    # Estatísticas
    upload_stats = {
        'total_uploads': 0,
        'successful_uploads': 0,
        'failed_uploads': 0,
        'databases_processed': [],
        'errors': []
    }
    
    try:
        # Criar conexão FTP
        ftp = ForecastFTPUploader()
        
        if not ftp._connect():
            print("❌ Falha ao conectar no servidor FTP")
            return {
                'success': False,
                'error': 'FTP connection failed',
                **upload_stats
            }
        
        print("✅ Conectado ao FTP com sucesso\n")
        
        # Iterar sobre cada database
        database_folders = [d for d in data_path.iterdir() if d.is_dir()]
        
        if not database_folders:
            print(f"⚠️  Nenhuma pasta de database encontrada em {data_dir}")
            ftp.disconnect()
            return {
                'success': False,
                'error': 'No database folders found',
                **upload_stats
            }
        
        print(f"📁 Encontradas {len(database_folders)} pastas de databases\n")
        
        for db_folder in database_folders:
            database_name = db_folder.name
            print(f"📊 Processando database: {database_name}")
            
            # Coletar arquivos parquet
            parquet_files = list(db_folder.glob('*.parquet'))
            
            if not parquet_files:
                print(f"   ⚠️  Nenhum arquivo parquet encontrado em {db_folder}")
                continue
            
            print(f"   📄 {len(parquet_files)} arquivos encontrados")
            
            # Converter para lista de strings (caminhos completos)
            file_paths = [str(f) for f in parquet_files]
            
            try:
                # Upload para FTP
                result = ftp.upload_data(
                    database_name=database_name,
                    forecast_type=forecast_type,
                    file_paths=file_paths
                )
                
                upload_stats['total_uploads'] += len(parquet_files)
                
                if result['success']:
                    upload_stats['successful_uploads'] += len(result['uploaded_files'])
                    upload_stats['databases_processed'].append(database_name)
                    print(f"   ✅ {result['message']}")
                else:
                    failed_count = len(result.get('failed_files', []))
                    upload_stats['failed_uploads'] += failed_count
                    upload_stats['successful_uploads'] += len(result['uploaded_files'])
                    print(f"   ⚠️  {result['message']}")
                    if result.get('failed_files'):
                        upload_stats['errors'].append(f"{database_name}: {failed_count} arquivos falharam")
                
            except Exception as e:
                error_msg = f"Erro no upload de {database_name}: {str(e)}"
                print(f"   ❌ {error_msg}")
                upload_stats['errors'].append(error_msg)
                upload_stats['failed_uploads'] += len(parquet_files)
            
            print()  # Linha em branco
        
        # Desconectar FTP
        ftp.disconnect()
        
        upload_stats['success'] = upload_stats['failed_uploads'] == 0
        return upload_stats
        
    except Exception as e:
        print(f"\n❌ Erro no processo de upload: {e}")
        return {
            'success': False,
            'error': str(e),
            **upload_stats
        }


def run_single_database_pipeline(
    database: str,
    output_dir: str = "data",
    verbose: bool = True,
    upload_ftp: bool = True,
    forecast_type: str = "data"
    ) -> Dict[str, Any]:
    """
    Executa pipeline de extração de dados SQL para um único database.
    
    Args:
        database: Nome do database para executar as queries
        output_dir: Diretório base para salvar os arquivos parquet
        verbose: Exibir logs detalhados
        upload_ftp: Fazer upload automático para FTP após extração
        forecast_type: Tipo de dados para FTP (usado se upload_ftp=True)
        
    Returns:
        Dicionário com estatísticas de execução SQL e FTP (se habilitado)
    """
    print("=" * 80)
    print(f"📊 PIPELINE ETL - DATABASE: {database}")
    print("=" * 80)
    
    try:
        # Instanciar SQLQuery
        extractor = SQLQuery()
        extractor.verbose = verbose
        
        # Executar queries para o database específico
        sql_results = extractor.execute_queries_for_database(
            database=database,
            output_dir=output_dir
        )
        
        # Verificar se houve sucesso na extração
        if not sql_results.get('success'):
            return {
                'success': False,
                'database': database,
                'sql_results': sql_results,
                'ftp_results': None
            }
        
        # Upload para FTP se habilitado e se houver dados extraídos
        ftp_results = None
        if upload_ftp and sql_results.get('successful', 0) > 0:
            print("\n" + "=" * 80)
            print("📤 UPLOAD PARA FTP/SFTP")
            print("=" * 80)
            
            try:
                ftp = ForecastFTPUploader()
                
                if not ftp._connect():
                    print("❌ Falha ao conectar no servidor FTP")
                    ftp_results = {
                        'success': False,
                        'error': 'FTP connection failed'
                    }
                else:
                    print("✅ Conectado ao FTP com sucesso\n")
                    
                    # Coletar arquivos parquet do database
                    db_dir = Path(output_dir) / database
                    parquet_files = list(db_dir.glob('*.parquet'))
                    
                    if parquet_files:
                        print(f"📄 {len(parquet_files)} arquivos encontrados")
                        file_paths = [str(f) for f in parquet_files]
                        
                        # Upload
                        result = ftp.upload_data(
                            database_name=database,
                            forecast_type=forecast_type,
                            file_paths=file_paths
                        )
                        
                        ftp_results = {
                            'success': result['success'],
                            'uploaded_files': len(result.get('uploaded_files', [])),
                            'failed_files': len(result.get('failed_files', [])),
                            'message': result['message']
                        }
                        
                        print(f"{'✅' if result['success'] else '⚠️'} {result['message']}")
                    else:
                        ftp_results = {
                            'success': False,
                            'error': 'No parquet files found'
                        }
                    
                    ftp.disconnect()
                    
            except Exception as e:
                print(f"❌ Erro no upload FTP: {e}")
                ftp_results = {
                    'success': False,
                    'error': str(e)
                }
        
        return {
            'success': True,
            'database': database,
            'sql_results': sql_results,
            'ftp_results': ftp_results
        }
        
    except Exception as e:
        print(f"\n❌ Erro no pipeline: {e}")
        return {
            'success': False,
            'database': database,
            'error': str(e),
            'sql_results': None,
            'ftp_results': None
        }


def run_single_database_supabase_pipeline(
    database: str,
    bucket_name: Optional[str] = None,
    verbose: bool = True,
    temp_dir: str = "temp"
    ) -> Dict[str, Any]:
    """
    Executa pipeline de extração de dados SQL para um único database e faz upload direto para Supabase.
    
    Args:
        database: Nome do database para executar as queries
        bucket_name: Nome do bucket Supabase (default: nome do database)
        verbose: Exibir logs detalhados
        temp_dir: Diretório temporário para processamento (default: "temp")
        
    Returns:
        Dicionário com estatísticas de execução SQL e Supabase
    """
    print("=" * 80)
    print(f"📊 PIPELINE ETL SUPABASE - DATABASE: {database}")
    print("=" * 80)
    
    # Determinar nome do bucket
    if bucket_name is None:
        bucket_name = database.lower().replace("_", "-")
    
    temp_path = None
    supabase_results = None
    
    try:
        # Instanciar SQLQuery
        extractor = SQLQuery()
        extractor.verbose = verbose
        
        # Executar queries para o database específico
        sql_results = extractor.execute_queries_for_database(
            database=database,
            output_dir=temp_dir
        )
        
        # Verificar se houve sucesso na extração
        if not sql_results.get('success'):
            return {
                'success': False,
                'database': database,
                'bucket_name': bucket_name,
                'sql_results': sql_results,
                'supabase_results': None
            }
        
        # Verificar se há dados extraídos
        if sql_results.get('successful', 0) == 0:
            print("⚠️  Nenhum dado extraído com sucesso. Pulando upload para Supabase.")
            return {
                'success': True,
                'database': database,
                'bucket_name': bucket_name,
                'sql_results': sql_results,
                'supabase_results': {
                    'success': False,
                    'error': 'No data extracted successfully'
                }
            }
        
        # Criar diretório temporário para o database
        temp_path = Path(temp_dir) / database
        
        if not temp_path.exists():
            print(f"❌ Diretório temporário {temp_path} não encontrado após extração SQL")
            return {
                'success': False,
                'database': database,
                'bucket_name': bucket_name,
                'sql_results': sql_results,
                'supabase_results': {
                    'success': False,
                    'error': 'Temporary directory not found after SQL extraction'
                }
            }
        
        # Verificar se há arquivos Parquet no diretório temporário
        parquet_files = list(temp_path.glob('*.parquet'))
        if not parquet_files:
            print(f"⚠️  Nenhum arquivo Parquet encontrado em {temp_path}")
            return {
                'success': True,
                'database': database,
                'bucket_name': bucket_name,
                'sql_results': sql_results,
                'supabase_results': {
                    'success': False,
                    'error': 'No parquet files found in temporary directory'
                }
            }
        
        print(f"📄 {len(parquet_files)} arquivos Parquet encontrados para upload")
        
        # Upload para Supabase
        print("\n" + "=" * 80)
        print("📤 UPLOAD PARA SUPABASE")
        print("=" * 80)
        
        try:
            # Inicializar SupabaseUploader
            uploader = SupabaseUploader()
            
            # Fazer upload em lote
            supabase_results = uploader.upload_directory_parquet(
                directory_path=str(temp_path),
                bucket_name=bucket_name
            )
            
            # Adicionar informações adicionais aos resultados
            supabase_results['success'] = supabase_results.get('failed_uploads', 0) == 0
            supabase_results['database'] = database
            supabase_results['bucket_name'] = bucket_name
            
            if supabase_results['success']:
                print(f"✅ Upload para Supabase concluído com sucesso!")
                print(f"   📁 Bucket: {bucket_name}")
                print(f"   📊 Arquivos enviados: {supabase_results['successful_uploads']}")
            else:
                print(f"⚠️  Upload para Supabase concluído com algumas falhas")
                print(f"   📁 Bucket: {bucket_name}")
                print(f"   ✅ Sucessos: {supabase_results['successful_uploads']}")
                print(f"   ❌ Falhas: {supabase_results['failed_uploads']}")
                
        except Exception as e:
            print(f"❌ Erro no upload para Supabase: {e}")
            supabase_results = {
                'success': False,
                'error': str(e),
                'database': database,
                'bucket_name': bucket_name,
                'total_files': 0,
                'successful_uploads': 0,
                'failed_uploads': 0
            }
        
        return {
            'success': True,
            'database': database,
            'bucket_name': bucket_name,
            'sql_results': sql_results,
            'supabase_results': supabase_results
        }
        
    except Exception as e:
        print(f"\n❌ Erro no pipeline Supabase: {e}")
        return {
            'success': False,
            'database': database,
            'bucket_name': bucket_name,
            'error': str(e),
            'sql_results': None,
            'supabase_results': None
        }
    
    finally:
        # Limpeza: remover diretório temporário se foi criado
        if temp_path and temp_path.exists():
            try:
                shutil.rmtree(temp_path)
                if verbose:
                    print(f"🧹 Diretório temporário {temp_path} removido com sucesso")
            except Exception as e:
                if verbose:
                    print(f"⚠️  Aviso: Não foi possível remover diretório temporário {temp_path}: {e}")


def print_summary(sql_results: Dict[str, Any], ftp_results: Dict[str, Any] = None):
    """
    Imprime resumo consolidado da execução.
    
    Args:
        sql_results: Resultados da extração SQL
        ftp_results: Resultados do upload FTP (opcional)
    """
    print("\n" + "=" * 80)
    print("📋 RESUMO FINAL DA EXECUÇÃO")
    print("=" * 80)
    
    # Resumo SQL
    print("\n📊 EXTRAÇÃO SQL:")
    if sql_results.get('success'):
        print(f"   ✅ Sucesso: {sql_results.get('successful', 0)}/{sql_results.get('total_executions', 0)} execuções")
        print(f"   ❌ Falhas: {sql_results.get('failed', 0)}/{sql_results.get('total_executions', 0)} execuções")
        if 'total_time' in sql_results:
            print(f"   ⏱️  Tempo total: {sql_results['total_time']:.2f}s")
    else:
        print(f"   ❌ Erro: {sql_results.get('error', 'Unknown error')}")
    
    # Resumo FTP
    if ftp_results:
        print("\n📤 UPLOAD FTP:")
        if ftp_results.get('success'):
            print(f"   ✅ Sucesso: {ftp_results.get('successful_uploads', 0)}/{ftp_results.get('total_uploads', 0)} arquivos")
            print(f"   📁 Databases processados: {len(ftp_results.get('databases_processed', []))}")
            if ftp_results.get('databases_processed'):
                for db in ftp_results['databases_processed']:
                    print(f"      - {db}")
        else:
            print(f"   ⚠️  Parcial: {ftp_results.get('successful_uploads', 0)}/{ftp_results.get('total_uploads', 0)} arquivos")
            print(f"   ❌ Falhas: {ftp_results.get('failed_uploads', 0)} arquivos")
            if ftp_results.get('error'):
                print(f"   ❌ Erro: {ftp_results['error']}")
        
        if ftp_results.get('errors'):
            print(f"\n   ⚠️  Erros de upload:")
            for error in ftp_results['errors']:
                print(f"      - {error}")
    
    print("\n" + "=" * 80)
    
    # Status geral
    overall_success = (
        sql_results.get('success', False) and 
        (ftp_results is None or ftp_results.get('successful_uploads', 0) > 0)
    )
    
    if overall_success:
        print("✅ Pipeline executado com sucesso!")
    else:
        print("⚠️  Pipeline executado com alguns problemas")
    
    print("=" * 80 + "\n")


if __name__ == '__main__':
    # """
    # Execução principal do pipeline ETL:
    # 1. Extração de dados SQL
    # 2. Upload para FTP/SFTP
    # 3. Resumo consolidado
    
    # Para testar o pipeline Supabase, descomente as linhas abaixo:
    # """
    # start_time = time.perf_counter()
    
    # print("\n🚀 INICIANDO PIPELINE ETL")
    # print(f"⏰ Início: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # # FASE 1: Extração de Dados SQL
    # sql_results = run_etl_pipeline(
    #     output_dir="data",
    #     verbose=True
    # )
    
    # # FASE 2: Upload para FTP (apenas se houver dados extraídos com sucesso)
    # ftp_results = None
    # if sql_results.get('successful', 0) > 0:
    #     ftp_results = upload_to_ftp(
    #         data_dir="data",
    #         forecast_type="data"
    #     )
    # else:
    #     print("\n⚠️  Nenhum dado extraído com sucesso. Pulando upload para FTP.")
    
    # # FASE 3: Resumo Final
    # total_time = time.perf_counter() - start_time
    # print_summary(sql_results, ftp_results)
    
    # print(f"⏱️  Tempo total de execução: {total_time:.2f}s")
    # print(f"⏰ Término: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # EXEMPLO DE USO DO PIPELINE SUPABASE (descomente para testar)
    print("\n" + "="*80)
    print("🧪 TESTE DO PIPELINE SUPABASE")
    print("="*80)
    
    # Testar pipeline Supabase para um database específico
    supabase_result = run_single_database_supabase_pipeline(
        database="013BW_ERP_BI",
        bucket_name="013bw-erp-bi",
        verbose=True,
        temp_dir="temp"
    )
    
    print("\n📊 RESULTADO PIPELINE SUPABASE:")
    print(f"   ✅ Sucesso: {supabase_result['success']}")
    print(f"   📁 Database: {supabase_result['database']}")
    print(f"   🪣 Bucket: {supabase_result['bucket_name']}")
    
    if supabase_result['supabase_results']:
        sb_results = supabase_result['supabase_results']
        print(f"   📊 Arquivos enviados: {sb_results.get('successful_uploads', 0)}")
        print(f"   ❌ Falhas: {sb_results.get('failed_uploads', 0)}")
    
    print("="*80)

