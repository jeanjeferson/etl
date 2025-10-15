"""
Módulo para envio de arquivos parquet para webhooks.
"""

import os
import logging
from pathlib import Path
from typing import Optional, Union, Dict, Any
import requests
import pandas as pd
from io import BytesIO


class WebhookParquetSender:
    """
    Classe para envio de arquivos parquet para webhooks.
    """
    
    def __init__(self, webhook_url: str, timeout: int = 30, 
                 username: Optional[str] = None, password: Optional[str] = None):
        """
        Inicializa o sender de webhook.
        
        Args:
            webhook_url: URL do webhook para envio dos arquivos
            timeout: Timeout em segundos para as requisições HTTP
            username: Nome de usuário para autenticação básica (opcional)
            password: Senha para autenticação básica (opcional)
        """
        self.webhook_url = webhook_url
        self.timeout = timeout
        self.username = username
        self.password = password
        self.logger = logging.getLogger(__name__)
        
    def send_parquet_file(self, file_path: Union[str, Path], 
                         additional_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Envia um arquivo parquet para o webhook.
        
        Args:
            file_path: Caminho para o arquivo parquet
            additional_fields: Campos adicionais para enviar junto com o arquivo
            
        Returns:
            Dict com informações sobre o resultado do envio
            
        Raises:
            FileNotFoundError: Se o arquivo não existir
            requests.RequestException: Se houver erro na requisição HTTP
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
        if not file_path.suffix.lower() == '.parquet':
            raise ValueError(f"Arquivo deve ser do tipo parquet, encontrado: {file_path.suffix}")
            
        self.logger.info(f"Enviando arquivo: {file_path.name} para {self.webhook_url}")
        
        try:
            # Preparar os dados para envio
            with open(file_path, 'rb') as file:
                files = {
                    'file': (file_path.name, file, 'application/octet-stream')
                }
                
                # Adicionar campos extras se fornecidos
                data = additional_fields or {}
                data.update({
                    'filename': file_path.name,
                    'file_size': file_path.stat().st_size,
                    'file_type': 'parquet'
                })
                
                # Preparar autenticação se fornecida
                auth = None
                if self.username and self.password:
                    auth = (self.username, self.password)
                
                # Fazer a requisição POST
                response = requests.post(
                    self.webhook_url,
                    files=files,
                    data=data,
                    auth=auth,
                    timeout=self.timeout
                )
                
                # Verificar se a requisição foi bem-sucedida
                response.raise_for_status()
                
                result = {
                    'success': True,
                    'status_code': response.status_code,
                    'filename': file_path.name,
                    'file_size': file_path.stat().st_size,
                    'response': response.json() if response.content else None
                }
                
                self.logger.info(f"Arquivo {file_path.name} enviado com sucesso. Status: {response.status_code}")
                return result
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Erro ao enviar arquivo {file_path.name}: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': str(e),
                'filename': file_path.name,
                'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            }
        except Exception as e:
            error_msg = f"Erro inesperado ao enviar arquivo {file_path.name}: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': str(e),
                'filename': file_path.name
            }
    
    def send_dataframe_as_parquet(self, df: pd.DataFrame, filename: str,
                                 additional_fields: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Envia um DataFrame pandas como arquivo parquet para o webhook.
        
        Args:
            df: DataFrame pandas para enviar
            filename: Nome do arquivo (deve terminar com .parquet)
            additional_fields: Campos adicionais para enviar junto com o arquivo
            
        Returns:
            Dict com informações sobre o resultado do envio
        """
        if not filename.endswith('.parquet'):
            filename += '.parquet'
            
        self.logger.info(f"Enviando DataFrame como {filename} para {self.webhook_url}")
        
        try:
            # Converter DataFrame para parquet em memória
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Preparar os dados para envio
            files = {
                'file': (filename, parquet_buffer, 'application/octet-stream')
            }
            
            # Adicionar campos extras se fornecidos
            data = additional_fields or {}
            data.update({
                'filename': filename,
                'file_size': len(parquet_buffer.getvalue()),
                'file_type': 'parquet',
                'rows_count': len(df),
                'columns_count': len(df.columns)
            })
            
            # Preparar autenticação se fornecida
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)
            
            # Fazer a requisição POST
            response = requests.post(
                self.webhook_url,
                files=files,
                data=data,
                auth=auth,
                timeout=self.timeout
            )
            
            # Verificar se a requisição foi bem-sucedida
            response.raise_for_status()
            
            result = {
                'success': True,
                'status_code': response.status_code,
                'filename': filename,
                'file_size': len(parquet_buffer.getvalue()),
                'rows_count': len(df),
                'columns_count': len(df.columns),
                'response': response.json() if response.content else None
            }
            
            self.logger.info(f"DataFrame enviado como {filename} com sucesso. Status: {response.status_code}")
            return result
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Erro ao enviar DataFrame como {filename}: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': str(e),
                'filename': filename,
                'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            }
        except Exception as e:
            error_msg = f"Erro inesperado ao enviar DataFrame como {filename}: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': str(e),
                'filename': filename
            }
    
    def test_send_clientes_file(self) -> Dict[str, Any]:
        """
        Método de teste para enviar o arquivo clientes.parquet.
        
        Returns:
            Dict com informações sobre o resultado do envio
        """
        # Caminho para o arquivo de teste
        test_file_path = Path(__file__).parent.parent / "data" / "013BW_ERP_BI" / "clientes.parquet"
        
        self.logger.info(f"Executando teste de envio do arquivo: {test_file_path}")
        
        if not test_file_path.exists():
            error_msg = f"Arquivo de teste não encontrado: {test_file_path}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'test_file_path': str(test_file_path)
            }
        
        # Enviar o arquivo
        result = self.send_parquet_file(
            test_file_path,
            additional_fields={
                'test_mode': True,
                'source': 'etl_system',
                'description': 'Arquivo de clientes para teste de webhook'
            }
        )
        
        # Adicionar informações específicas do teste
        result['test_file_path'] = str(test_file_path)
        result['test_mode'] = True
        
        return result


def main():
    """
    Função principal para teste da classe.
    """
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # URL do webhook
    webhook_url = "https://automations-n8n-webhook.gxlml1.easypanel.host/webhook/send-file"
    
    # Criar instância do sender (com autenticação se disponível)
    # IMPORTANTE: Configure as credenciais de autenticação aqui
    username = None  # Substitua por seu username
    password = None  # Substitua por sua senha
    
    sender = WebhookParquetSender(webhook_url, username=username, password=password)
    
    # Executar teste
    print("Iniciando teste de envio do arquivo clientes.parquet...")
    print(f"URL do webhook: {webhook_url}")
    print(f"Autenticação: {'Configurada' if username and password else 'Não configurada'}")
    
    result = sender.test_send_clientes_file()
    
    # Mostrar resultado
    print("\n" + "="*50)
    print("RESULTADO DO TESTE:")
    print("="*50)
    
    for key, value in result.items():
        print(f"{key}: {value}")
    
    if result.get('success'):
        print("\n✅ Teste executado com sucesso!")
    else:
        print(f"\n❌ Teste falhou: {result.get('error', 'Erro desconhecido')}")
        if "403" in str(result.get('error', '')):
            print("\n💡 DICA: O webhook requer autenticação básica.")
            print("   Configure username e password na função main() para resolver o problema.")


if __name__ == "__main__":
    main()
