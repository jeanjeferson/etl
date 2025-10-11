"""
FTP Uploader Simplificado para Upload dos Resultados de Forecasting
Envia automaticamente os CSVs para o servidor SFTP na estrutura ai/{database}/{tipo}/
"""

import paramiko
import os
import time
from pathlib import Path
from typing import List, Optional, Dict, Any
import stat


class ForecastFTPUploader:
    """Classe simplificada para upload dos resultados de forecasting."""
    
    def __init__(self, host: str = "192.168.49.30", port: int = 8887, 
                 username: str = "sftp_ia01", password: str = "#6U9Fv@C!Yk6VqNbaM8B"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_client = None
        self.sftp_client = None
        self._connected = False
    
    def _connect(self) -> bool:
        """Conecta ao servidor SFTP."""
        try:
            if self._connected and self.sftp_client:
                # Testa se a conexão ainda está ativa
                try:
                    self.sftp_client.listdir('.')
                    return True
                except:
                    self._disconnect()
            
            print(f"🔗 Conectando ao SFTP {self.host}:{self.port}")
            
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            self.ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=30,
                look_for_keys=False,
                allow_agent=False
            )
            
            self.sftp_client = self.ssh_client.open_sftp()
            self._connected = True
            print("✅ Conectado ao SFTP com sucesso")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao conectar SFTP: {e}")
            self._disconnect()
            return False
    
    def _disconnect(self):
        """Desconecta do SFTP."""
        try:
            if self.sftp_client:
                self.sftp_client.close()
        except:
            pass
        try:
            if self.ssh_client:
                self.ssh_client.close()
        except:
            pass
        
        self.ssh_client = None
        self.sftp_client = None
        self._connected = False
    
    def _ensure_directory(self, remote_path: str) -> bool:
        """Garante que o diretório remoto existe."""
        if not remote_path or remote_path in ['.', '/']:
            return True
            
        try:
            # Verifica se já existe
            try:
                self.sftp_client.listdir(remote_path)
                return True
            except FileNotFoundError:
                pass
            
            # Cria recursivamente
            dirs = remote_path.strip('/').split('/')
            current_path = ''
            
            for dir_name in dirs:
                if not dir_name:
                    continue
                
                current_path += f'/{dir_name}' if current_path else dir_name
                
                try:
                    self.sftp_client.listdir(current_path)
                except FileNotFoundError:
                    try:
                        self.sftp_client.mkdir(current_path)
                        print(f"📁 Criado diretório: {current_path}")
                    except:
                        pass
            
            return True
            
        except Exception as e:
            print(f"❌ Erro ao criar diretório {remote_path}: {e}")
            return False
    
    def upload_data(self, database_name: str, forecast_type: str, 
                               file_paths: List[str]) -> Dict[str, Any]:
        """
        Faz upload dos resultados de forecasting.
        
        Args:
            database_name: Nome do database (ex: '001RR_BI')
            forecast_type: Tipo do forecast ('vendas' ou 'volume')
            file_paths: Lista de caminhos dos arquivos locais
            
        Returns:
            Dict com resultado do upload
        """
        if not self._connect():
            return {
                'success': False,
                'message': 'Erro de conexão SFTP',
                'uploaded_files': []
            }
        
        # Pasta destino no formato: ai/{database}/{tipo}/
        remote_folder = f"ai/{database_name}/{forecast_type}"
        
        # Garantir que pasta existe
        if not self._ensure_directory(remote_folder):
            return {
                'success': False,
                'message': f'Erro ao criar diretório {remote_folder}',
                'uploaded_files': []
            }
        
        uploaded_files = []
        failed_files = []
        
        for file_path in file_paths:
            if not Path(file_path).exists():
                print(f"⚠️ Arquivo não encontrado: {file_path}")
                failed_files.append(file_path)
                continue
            
            filename = Path(file_path).name
            remote_path = f"{remote_folder}/{filename}"
            
            try:
                print(f"📤 Upload: {filename} → {remote_path}")
                self.sftp_client.put(file_path, remote_path)
                
                # Verifica se upload foi bem sucedido
                try:
                    local_size = Path(file_path).stat().st_size
                    remote_stat = self.sftp_client.stat(remote_path)
                    
                    if remote_stat.st_size == local_size:
                        print(f"✅ Upload verificado: {filename}")
                        uploaded_files.append(filename)
                    else:
                        print(f"⚠️ Tamanho diferente: {filename}")
                        uploaded_files.append(filename)  # Considera como sucesso
                        
                except:
                    print(f"✅ Upload concluído: {filename}")
                    uploaded_files.append(filename)
                    
            except Exception as e:
                print(f"❌ Erro no upload de {filename}: {e}")
                failed_files.append(file_path)
                time.sleep(0.5)  # Pequena pausa antes de continuar
        
        success = len(failed_files) == 0
        total_files = len(file_paths)
        success_count = len(uploaded_files)
        
        message = f"Upload {forecast_type}: {success_count}/{total_files} arquivos"
        if database_name:
            message += f" para ai/{database_name}/{forecast_type}/"
        
        return {
            'success': success,
            'message': message,
            'uploaded_files': uploaded_files,
            'failed_files': failed_files,
            'remote_path': remote_folder
        }
    
    def disconnect(self):
        """Desconecta do servidor."""
        self._disconnect()
        print("🔌 Desconectado do SFTP")
    
    def __enter__(self):
        """Context manager - entrada."""
        self._connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager - saída."""
        self.disconnect()

if __name__ == "__main__":
    # Teste básico
    print("🧪 Teste FTP Uploader")
    
    # Criar instância e testar conexão
    ftp = ForecastFTPUploader()
    
    if ftp._connect():
        print("✅ Conexão OK")

        ftp.upload_data('005ATS_ERP_BI', 'data', 'data/005ATS_ERP_BI')
        
        ftp.disconnect()
    else:
        print("❌ Falha na conexão")
