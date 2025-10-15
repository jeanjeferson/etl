"""
Supabase Storage Uploader for Parquet Files

This module provides a simple class for uploading and downloading Parquet files
to/from Supabase storage buckets.
"""

import os
import sys
from typing import Optional, Union
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

class SupabaseUploader:
    """
    A simple class for handling Parquet file uploads and downloads to Supabase storage.
    
    This class provides methods to upload Parquet files to Supabase storage buckets
    and download them back to local storage.
    
    Configuration is loaded from environment variables:
    - SUPABASE_URL: Supabase project URL
    - SUPABASE_KEY: Supabase service role key
    """
    
    # Default Supabase configuration (fallback values)
    DEFAULT_URL = "https://supabase.agendai.cc/"
    DEFAULT_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogInNlcnZpY2Vfcm9sZSIsCiAgImlzcyI6ICJzdXBhYmFzZSIsCiAgImlhdCI6IDE3NDM5OTQ4MDAsCiAgImV4cCI6IDE5MDE3NjEyMDAKfQ.CcQ_oefiHWsvTbtGzq9GL6kRu5uv38U8oS6HSKeG2Ao"
    
    def __init__(self, url: Optional[str] = None, key: Optional[str] = None):
        """
        Initialize the Supabase client.
        
        Loads configuration from environment variables with fallback to defaults.
        Environment variables:
        - SUPABASE_URL: Supabase project URL
        - SUPABASE_KEY: Supabase service role key
        
        Args:
            url: Supabase URL (overrides environment variable and default)
            key: Supabase service key (overrides environment variable and default)
        """
        # Load environment variables from .env file
        load_dotenv()
        
        # Get configuration from environment variables or use defaults
        self.url = url or os.getenv("SUPABASE_URL", self.DEFAULT_URL)
        self.key = key or os.getenv("SUPABASE_KEY", self.DEFAULT_KEY)
        
        # Validate configuration
        if not self.url or not self.key:
            raise ValueError("Supabase URL and KEY must be provided either as parameters or environment variables")
        
        self.supabase: Client = create_client(self.url, self.key)
    
    def upload_parquet(self, bucket_name: str, file_path: str) -> Optional[dict]:
        """
        Upload a Parquet file to Supabase storage.
        
        Args:
            bucket_name: Name of the Supabase storage bucket
            file_path: Local path to the Parquet file
            
        Returns:
            Response dictionary from Supabase on success, None on failure
        """
        try:
            # Verify file exists before attempting upload
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Arquivo {file_path} não encontrado para upload")
            
            # Read file data
            with open(file_path, "rb") as f:
                file_data = f.read()
            
            # Extract only the filename for storage path
            file_name = os.path.basename(file_path)
            
            # Upload to Supabase storage
            response = self.supabase.storage.from_(bucket_name).upload(
                file_name,  # Use only the filename
                file_data,
                {'upsert': 'true'}  # Allow overwriting existing files
            )
            
            print(f"Arquivo {file_name} enviado com sucesso para o bucket {bucket_name}")
            return response
            
        except Exception as e:
            print(f"Erro no upload do arquivo {file_path}: {str(e)}")
            return None
    
    def download_parquet(self, bucket_name: str, file_name: str, local_path: str) -> Optional[pd.DataFrame]:
        """
        Download a Parquet file from Supabase storage and return as DataFrame.
        
        Args:
            bucket_name: Name of the Supabase storage bucket
            file_name: Name of the file in the bucket
            local_path: Local path where to save the downloaded file
            
        Returns:
            Pandas DataFrame on success, None on failure
        """
        try:
            # Download file from Supabase storage
            response = self.supabase.storage.from_(bucket_name).download(file_name)
            
            if response is None:
                raise Exception("Arquivo não encontrado no storage")
            
            # Create local directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Write file locally
            with open(local_path, "wb") as f:
                f.write(response)
            
            print(f"Arquivo {file_name} baixado com sucesso em: {local_path}")
            
            # Read and return as DataFrame
            file_data = pd.read_parquet(local_path)
            return file_data
            
        except Exception as e:
            print(f"Erro no download do arquivo {file_name}: {str(e)}")
            return None
    
    def list_files(self, bucket_name: str, folder_path: str = "") -> Optional[list]:
        """
        List files in a Supabase storage bucket.
        
        Args:
            bucket_name: Name of the Supabase storage bucket
            folder_path: Optional folder path within the bucket
            
        Returns:
            List of files on success, None on failure
        """
        try:
            response = self.supabase.storage.from_(bucket_name).list(folder_path)
            return response
        except Exception as e:
            print(f"Erro ao listar arquivos no bucket {bucket_name}: {str(e)}")
            return None
    
    def delete_file(self, bucket_name: str, file_name: str) -> bool:
        """
        Delete a file from Supabase storage.
        
        Args:
            bucket_name: Name of the Supabase storage bucket
            file_name: Name of the file to delete
            
        Returns:
            True on success, False on failure
        """
        try:
            response = self.supabase.storage.from_(bucket_name).remove([file_name])
            print(f"Arquivo {file_name} removido com sucesso do bucket {bucket_name}")
            return True
        except Exception as e:
            print(f"Erro ao remover arquivo {file_name}: {str(e)}")
            return False
    
    def upload_directory_parquet(self, directory_path: str, bucket_name: str) -> dict:
        """
        Upload all Parquet files from a directory to Supabase storage.
        
        Args:
            directory_path: Path to the directory containing Parquet files
            bucket_name: Name of the Supabase storage bucket
            
        Returns:
            dict: Summary with success count, failure count, and details
        """
        import glob
        
        # Find all .parquet files in the directory
        parquet_pattern = os.path.join(directory_path, "*.parquet")
        parquet_files = glob.glob(parquet_pattern)
        
        if not parquet_files:
            print(f"❌ Nenhum arquivo .parquet encontrado em {directory_path}")
            return {
                "total_files": 0,
                "successful_uploads": 0,
                "failed_uploads": 0,
                "successful_files": [],
                "failed_files": []
            }
        
        print(f"📁 Encontrados {len(parquet_files)} arquivos .parquet em {directory_path}")
        print("=" * 60)
        
        # Initialize counters
        successful_uploads = 0
        failed_uploads = 0
        successful_files = []
        failed_files = []
        
        # Process each file
        for i, file_path in enumerate(parquet_files, 1):
            file_name = os.path.basename(file_path)
            print(f"📤 [{i}/{len(parquet_files)}] Processando: {file_name}")
            
            try:
                result = self.upload_parquet(bucket_name, file_path)
                if result:
                    successful_uploads += 1
                    successful_files.append(file_name)
                    print(f"   ✅ {file_name} - Upload realizado com sucesso")
                else:
                    failed_uploads += 1
                    failed_files.append(file_name)
                    print(f"   ❌ {file_name} - Falha no upload")
            except Exception as e:
                failed_uploads += 1
                failed_files.append(file_name)
                print(f"   ❌ {file_name} - Erro: {str(e)}")
            
            print()  # Empty line for readability
        
        # Print summary
        print("=" * 60)
        print("📊 RESUMO DO UPLOAD")
        print("=" * 60)
        print(f"📁 Total de arquivos: {len(parquet_files)}")
        print(f"✅ Uploads bem-sucedidos: {successful_uploads}")
        print(f"❌ Uploads com falha: {failed_uploads}")
        
        if successful_files:
            print(f"\n✅ Arquivos enviados com sucesso:")
            for file_name in successful_files:
                print(f"   • {file_name}")
        
        if failed_files:
            print(f"\n❌ Arquivos com falha:")
            for file_name in failed_files:
                print(f"   • {file_name}")
        
        return {
            "total_files": len(parquet_files),
            "successful_uploads": successful_uploads,
            "failed_uploads": failed_uploads,
            "successful_files": successful_files,
            "failed_files": failed_files
        }
    
    def download_directory_parquet(self, bucket_name: str, local_directory: str, file_filter: str = "*") -> dict:
        """
        Download all Parquet files from a Supabase storage bucket to a local directory.
        
        Args:
            bucket_name: Name of the Supabase storage bucket
            local_directory: Local directory to save downloaded files
            file_filter: Filter pattern for files (default: "*" for all files)
            
        Returns:
            dict: Summary with success count, failure count, and details
        """
        try:
            # List all files in the bucket
            files_in_bucket = self.list_files(bucket_name)
            
            if not files_in_bucket:
                print(f"❌ Nenhum arquivo encontrado no bucket {bucket_name}")
                return {
                    "total_files": 0,
                    "successful_downloads": 0,
                    "failed_downloads": 0,
                    "successful_files": [],
                    "failed_files": [],
                    "local_directory": local_directory
                }
            
            # Filter files if needed
            if file_filter != "*":
                import fnmatch
                files_in_bucket = [f for f in files_in_bucket if fnmatch.fnmatch(f.get('name', ''), file_filter)]
            
            if not files_in_bucket:
                print(f"❌ Nenhum arquivo correspondente ao filtro '{file_filter}' encontrado no bucket {bucket_name}")
                return {
                    "total_files": 0,
                    "successful_downloads": 0,
                    "failed_downloads": 0,
                    "successful_files": [],
                    "failed_files": [],
                    "local_directory": local_directory
                }
            
            print(f"📁 Encontrados {len(files_in_bucket)} arquivo(s) no bucket {bucket_name}")
            print(f"📂 Diretório local: {local_directory}")
            print("=" * 60)
            
            # Create local directory if it doesn't exist
            os.makedirs(local_directory, exist_ok=True)
            
            # Initialize counters
            successful_downloads = 0
            failed_downloads = 0
            successful_files = []
            failed_files = []
            
            # Process each file
            for i, file_info in enumerate(files_in_bucket, 1):
                file_name = file_info.get('name', '')
                print(f"📥 [{i}/{len(files_in_bucket)}] Baixando: {file_name}")
                
                try:
                    # Construct local file path
                    local_file_path = os.path.join(local_directory, file_name)
                    
                    # Download file
                    df = self.download_parquet(bucket_name, file_name, local_file_path)
                    
                    if df is not None:
                        successful_downloads += 1
                        successful_files.append(file_name)
                        print(f"   ✅ {file_name} - Download realizado com sucesso")
                    else:
                        failed_downloads += 1
                        failed_files.append(file_name)
                        print(f"   ❌ {file_name} - Falha no download")
                        
                except Exception as e:
                    failed_downloads += 1
                    failed_files.append(file_name)
                    print(f"   ❌ {file_name} - Erro: {str(e)}")
                
                print()  # Empty line for readability
            
            # Print summary
            print("=" * 60)
            print("📊 RESUMO DO DOWNLOAD")
            print("=" * 60)
            print(f"📁 Total de arquivos: {len(files_in_bucket)}")
            print(f"✅ Downloads bem-sucedidos: {successful_downloads}")
            print(f"❌ Downloads com falha: {failed_downloads}")
            print(f"📂 Diretório local: {local_directory}")
            
            if successful_files:
                print(f"\n✅ Arquivos baixados com sucesso:")
                for file_name in successful_files:
                    print(f"   • {file_name}")
            
            if failed_files:
                print(f"\n❌ Arquivos com falha:")
                for file_name in failed_files:
                    print(f"   • {file_name}")
            
            return {
                "total_files": len(files_in_bucket),
                "successful_downloads": successful_downloads,
                "failed_downloads": failed_downloads,
                "successful_files": successful_files,
                "failed_files": failed_files,
                "local_directory": local_directory
            }
            
        except Exception as e:
            print(f"❌ Erro durante o download em lote: {str(e)}")
            return {
                "total_files": 0,
                "successful_downloads": 0,
                "failed_downloads": 0,
                "successful_files": [],
                "failed_files": [],
                "local_directory": local_directory,
                "error": str(e)
            }



def show_menu():
    """
    Display interactive menu and get user selection.
    
    Returns:
        str: Selected mode ('upload', 'download', or 'exit')
    """
    while True:
        print("\n" + "=" * 60)
        print("=== SupabaseUploader - Menu Interativo ===")
        print("=" * 60)
        print("🪣 Bucket: 013bw-erp-bi")
        print("📁 Diretório: data/013BW_ERP_BI")
        print("📂 Download dir: downloads/013BW_ERP_BI")
        print()
        print("Escolha uma opção:")
        print("1. 📤 Upload arquivos para Supabase")
        print("2. 📥 Download arquivos do Supabase")
        print("3. ❌ Sair")
        print()
        
        try:
            choice = input("Digite sua opção (1-3): ").strip()
            
            if choice == "1":
                return "upload"
            elif choice == "2":
                return "download"
            elif choice == "3":
                return "exit"
            else:
                print("❌ Opção inválida! Digite 1, 2 ou 3.")
                input("Pressione Enter para continuar...")
                
        except KeyboardInterrupt:
            print("\n\n👋 Operação cancelada pelo usuário.")
            return "exit"
        except EOFError:
            print("\n\n👋 Operação cancelada.")
            return "exit"


if __name__ == "__main__":
    """
    Test script for SupabaseUploader class.
    Interactive menu for upload and download functionality.
    """ 
    
    # Configuration
    directory_path = "data/013BW_ERP_BI"
    bucket_name = "013bw-erp-bi"
    download_dir = "downloads/013BW_ERP_BI"
    
    # Show welcome message
    print("🚀 Bem-vindo ao SupabaseUploader!")
    print("Este script permite fazer upload e download de arquivos Parquet para/do Supabase.")
    
    # Main loop
    while True:
        try:
            # Get user selection
            mode = show_menu()
            
            if mode == "exit":
                print("\n👋 Obrigado por usar o SupabaseUploader!")
                break
            
            print(f"\n🔧 Modo selecionado: {mode.upper()}")
            print("=" * 50)
            
            try:
                # Initialize uploader
                print("🔧 Inicializando SupabaseUploader...")
                uploader = SupabaseUploader()
                print("✅ SupabaseUploader inicializado com sucesso!")
                print()
                
                if mode == "upload":
                    # UPLOAD MODE
                    if not os.path.exists(directory_path):
                        print(f"❌ Erro: Diretório {directory_path} não encontrado!")
                        print("Verifique se o diretório existe no caminho especificado.")
                        input("Pressione Enter para continuar...")
                        continue
                    
                    print("🚀 Iniciando upload em lote...")
                    result = uploader.upload_directory_parquet(directory_path, bucket_name)
                    
                    # Final summary
                    print("\n" + "=" * 50)
                    print("🎯 RESULTADO FINAL - UPLOAD")
                    print("=" * 50)
                    
                    if result["successful_uploads"] > 0:
                        print(f"✅ {result['successful_uploads']} arquivo(s) enviado(s) com sucesso!")
                    
                    if result["failed_uploads"] > 0:
                        print(f"❌ {result['failed_uploads']} arquivo(s) com falha!")
                    
                    if result["total_files"] == 0:
                        print("⚠️  Nenhum arquivo .parquet encontrado no diretório!")
                    
                    print(f"\n📊 Estatísticas:")
                    print(f"   • Total processado: {result['total_files']}")
                    print(f"   • Sucessos: {result['successful_uploads']}")
                    print(f"   • Falhas: {result['failed_uploads']}")
                    
                elif mode == "download":
                    # DOWNLOAD MODE
                    print("📥 Iniciando download em lote...")
                    result = uploader.download_directory_parquet(bucket_name, download_dir)
                    
                    # Final summary
                    print("\n" + "=" * 50)
                    print("🎯 RESULTADO FINAL - DOWNLOAD")
                    print("=" * 50)
                    
                    if result["successful_downloads"] > 0:
                        print(f"✅ {result['successful_downloads']} arquivo(s) baixado(s) com sucesso!")
                    
                    if result["failed_downloads"] > 0:
                        print(f"❌ {result['failed_downloads']} arquivo(s) com falha!")
                    
                    if result["total_files"] == 0:
                        print("⚠️  Nenhum arquivo encontrado no bucket!")
                    
                    print(f"\n📊 Estatísticas:")
                    print(f"   • Total processado: {result['total_files']}")
                    print(f"   • Sucessos: {result['successful_downloads']}")
                    print(f"   • Falhas: {result['failed_downloads']}")
                    print(f"   • Diretório local: {result['local_directory']}")
                
                # Ask if user wants to continue
                print("\n" + "=" * 50)
                try:
                    continue_choice = input("Deseja realizar outra operação? (s/n): ").strip().lower()
                    if continue_choice not in ['s', 'sim', 'y', 'yes']:
                        print("\n👋 Obrigado por usar o SupabaseUploader!")
                        break
                except (KeyboardInterrupt, EOFError):
                    print("\n\n👋 Obrigado por usar o SupabaseUploader!")
                    break
                    
            except Exception as e:
                print(f"❌ Erro durante o processo: {str(e)}")
                print("\n💡 Dicas para resolução:")
                print("- Verifique se as variáveis SUPABASE_URL e SUPABASE_KEY estão configuradas no .env")
                print("- Certifique-se de que o bucket existe no Supabase")
                print("- Verifique sua conexão com a internet")
                print("- Para upload: confirme se o diretório contém arquivos .parquet válidos")
                print("- Para download: confirme se o bucket contém arquivos")
                
                try:
                    input("Pressione Enter para continuar...")
                except (KeyboardInterrupt, EOFError):
                    print("\n\n👋 Obrigado por usar o SupabaseUploader!")
                    break
                    
        except KeyboardInterrupt:
            print("\n\n👋 Operação cancelada pelo usuário.")
            break
        except EOFError:
            print("\n\n👋 Operação cancelada.")
            break
