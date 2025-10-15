"""
FastAPI ETL Pipeline Runner
API para executar pipeline ETL de forma assíncrona
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

from run_sql import run_etl_pipeline, upload_to_ftp, run_single_database_pipeline, run_single_database_supabase_pipeline
from utils.upload_supabase import SupabaseUploader


# FastAPI app instance
app = FastAPI(
    title="ETL Pipeline API",
    description="API para executar pipeline de extração SQL e upload FTP de forma assíncrona",
    version="1.0.0"
    )


# In-memory job storage
jobs: Dict[str, Dict[str, Any]] = {}


# Pydantic models
class JobStatus(BaseModel):
    job_id: str
    status: str = Field(..., description="Status do job: running, completed, failed")
    message: str
    started_at: str


class JobDetail(BaseModel):
    job_id: str
    status: str
    started_at: str
    completed_at: Optional[str] = None
    sql_results: Optional[Dict[str, Any]] = None
    ftp_results: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class HealthCheck(BaseModel):
    status: str
    service: str


class SupabaseUploadRequest(BaseModel):
    bucket_name: Optional[str] = Field(None, description="Nome do bucket Supabase (default: 013bw-erp-bi )")
    output_dir: str = Field("data", description="Diretório base contendo os arquivos parquet")


class SupabaseUploadResponse(BaseModel):
    job_id: str
    status: str
    message: str
    started_at: str
    database: str
    bucket_name: str


class SupabasePipelineRequest(BaseModel):
    bucket_name: Optional[str] = Field(None, description="Nome do bucket Supabase (default: 013bw-erp-bi )")
    verbose: bool = Field(True, description="Exibir logs detalhados")
    temp_dir: str = Field("temp", description="Diretório temporário para processamento")


class SupabasePipelineResponse(BaseModel):
    job_id: str
    status: str
    message: str
    started_at: str
    database: str
    bucket_name: str


# Background task function
def execute_pipeline_task(
    job_id: str,
    output_dir: str,
    forecast_type: str,
    verbose: bool
    ):
    """
    Executa o pipeline ETL em background e atualiza o status do job.
    
    Args:
        job_id: ID único do job
        output_dir: Diretório para salvar arquivos parquet
        forecast_type: Tipo de dados para FTP
        verbose: Exibir logs detalhados
    """
    try:
        # Atualizar status para running
        jobs[job_id]["status"] = "running"
        
        # FASE 1: Extração SQL
        sql_results = run_etl_pipeline(
            output_dir=output_dir,
            verbose=verbose
        )
        
        jobs[job_id]["sql_results"] = sql_results
        
        # FASE 2: Upload FTP (apenas se houver dados extraídos)
        ftp_results = None
        if sql_results.get('successful', 0) > 0:
            ftp_results = upload_to_ftp(
                data_dir=output_dir,
                forecast_type=forecast_type
            )
            jobs[job_id]["ftp_results"] = ftp_results
        
        # Atualizar status para completed
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["completed_at"] = datetime.now().isoformat()
        
    except Exception as e:
        # Atualizar status para failed
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["completed_at"] = datetime.now().isoformat()


def execute_single_database_task(
    job_id: str,
    database: str,
    output_dir: str,
    forecast_type: str,
    verbose: bool,
    upload_ftp: bool
    ):
    """
    Executa o pipeline ETL para um único database em background.
    
    Args:
        job_id: ID único do job
        database: Nome do database
        output_dir: Diretório para salvar arquivos parquet
        forecast_type: Tipo de dados para FTP
        verbose: Exibir logs detalhados
        upload_ftp: Fazer upload automático para FTP
    """
    try:
        # Atualizar status para running
        jobs[job_id]["status"] = "running"
        
        # Executar pipeline para database específico
        results = run_single_database_pipeline(
            database=database,
            output_dir=output_dir,
            verbose=verbose,
            upload_ftp=upload_ftp,
            forecast_type=forecast_type
        )
        
        jobs[job_id]["sql_results"] = results.get("sql_results")
        jobs[job_id]["ftp_results"] = results.get("ftp_results")
        
        # Atualizar status
        if results.get("success"):
            jobs[job_id]["status"] = "completed"
        else:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = results.get("error", "Unknown error")
        
        jobs[job_id]["completed_at"] = datetime.now().isoformat()
        
    except Exception as e:
        # Atualizar status para failed
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["completed_at"] = datetime.now().isoformat()


def execute_supabase_upload_task(
    job_id: str,
    database: str,
    bucket_name: str,
    output_dir: str
    ):
    """
    Executa o upload de arquivos Parquet para Supabase em background.
    
    Args:
        job_id: ID único do job
        database: Nome do database
        bucket_name: Nome do bucket Supabase
        output_dir: Diretório base contendo os arquivos parquet
    """
    try:
        # Atualizar status para running
        jobs[job_id]["status"] = "running"
        
        # Construir caminho do diretório do database
        database_dir = f"{output_dir}/{database}"
        
        # Inicializar SupabaseUploader
        uploader = SupabaseUploader()
        
        # Executar upload em lote
        upload_results = uploader.upload_directory_parquet(
            directory_path=database_dir,
            bucket_name=bucket_name
        )
        
        # Armazenar resultados
        jobs[job_id]["supabase_results"] = upload_results
        
        # Atualizar status baseado nos resultados
        if upload_results["failed_uploads"] == 0:
            jobs[job_id]["status"] = "completed"
        else:
            jobs[job_id]["status"] = "completed"  # Ainda considera completo, mas com falhas
            jobs[job_id]["error"] = f"{upload_results['failed_uploads']} arquivo(s) falharam no upload"
        
        jobs[job_id]["completed_at"] = datetime.now().isoformat()
        
    except Exception as e:
        # Atualizar status para failed
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["completed_at"] = datetime.now().isoformat()


def execute_supabase_pipeline_task(
    job_id: str,
    database: str,
    bucket_name: str,
    verbose: bool,
    temp_dir: str
    ):
    """
    Executa o pipeline ETL Supabase para um database específico em background.
    
    Args:
        job_id: ID único do job
        database: Nome do database
        bucket_name: Nome do bucket Supabase
        verbose: Exibir logs detalhados
        temp_dir: Diretório temporário para processamento
    """
    try:
        # Atualizar status para running
        jobs[job_id]["status"] = "running"
        
        # Executar pipeline Supabase
        results = run_single_database_supabase_pipeline(
            database=database,
            bucket_name=bucket_name,
            verbose=verbose,
            temp_dir=temp_dir
        )
        
        jobs[job_id]["sql_results"] = results.get("sql_results")
        jobs[job_id]["supabase_results"] = results.get("supabase_results")
        
        # Atualizar status baseado nos resultados
        if results.get("success"):
            jobs[job_id]["status"] = "completed"
        else:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = results.get("error", "Unknown error")
        
        jobs[job_id]["completed_at"] = datetime.now().isoformat()
        
    except Exception as e:
        # Atualizar status para failed
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        jobs[job_id]["completed_at"] = datetime.now().isoformat()


# Endpoints
@app.get("/", tags=["Root"])
async def root():
    """Endpoint raiz com informações básicas da API."""
    return {
        "service": "ETL Pipeline API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthCheck, tags=["Health"])
async def health_check():
    """
    Health check endpoint para verificar se a API está funcionando.
    """
    return {
        "status": "healthy",
        "service": "ETL Pipeline API"
    }


@app.post("/run-pipeline", response_model=JobStatus, tags=["Pipeline"])
async def run_pipeline(
    background_tasks: BackgroundTasks,
    output_dir: str = "data",
    forecast_type: str = "data",
    verbose: bool = True
    ):
    """
    Inicia a execução do pipeline ETL em background.
    
    Args:
        output_dir: Diretório base para salvar arquivos parquet (default: "data")
        forecast_type: Tipo de dados para FTP (default: "data")
        verbose: Exibir logs detalhados (default: True)
        
    Returns:
        JobStatus com job_id e status inicial
    """
    # Gerar job ID único
    job_id = str(uuid.uuid4())
    
    # Criar entrada no storage de jobs
    jobs[job_id] = {
        "status": "pending",
        "started_at": datetime.now().isoformat(),
        "output_dir": output_dir,
        "forecast_type": forecast_type,
        "verbose": verbose,
        "sql_results": None,
        "ftp_results": None,
        "error": None,
        "completed_at": None
    }
    
    # Adicionar tarefa em background
    background_tasks.add_task(
        execute_pipeline_task,
        job_id=job_id,
        output_dir=output_dir,
        forecast_type=forecast_type,
        verbose=verbose
    )
    
    return {
        "job_id": job_id,
        "status": "pending",
        "message": "Pipeline ETL iniciado em background",
        "started_at": jobs[job_id]["started_at"]
    }


@app.get("/jobs/{job_id}", response_model=JobDetail, tags=["Jobs"])
async def get_job_status(job_id: str):
    """
    Obtém o status e resultados de um job específico.
    
    Args:
        job_id: ID do job
        
    Returns:
        JobDetail com informações completas do job
        
    Raises:
        HTTPException 404: Job não encontrado
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} não encontrado")
    
    job_data = jobs[job_id]
    
    return {
        "job_id": job_id,
        "status": job_data["status"],
        "started_at": job_data["started_at"],
        "completed_at": job_data.get("completed_at"),
        "sql_results": job_data.get("sql_results"),
        "ftp_results": job_data.get("ftp_results"),
        "supabase_results": job_data.get("supabase_results"),
        "error": job_data.get("error")
    }


@app.get("/jobs", tags=["Jobs"])
async def list_jobs():
    """
    Lista todos os jobs e seus status.
    
    Returns:
        Lista de jobs com informações resumidas
    """
    jobs_list = []
    for job_id, job_data in jobs.items():
        jobs_list.append({
            "job_id": job_id,
            "status": job_data["status"],
            "started_at": job_data["started_at"],
            "completed_at": job_data.get("completed_at"),
        "output_dir": job_data.get("output_dir"),
        "forecast_type": job_data.get("forecast_type"),
        "database": job_data.get("database"),
        "bucket_name": job_data.get("bucket_name")
        })
    
    # Ordenar por data de início (mais recente primeiro)
    jobs_list.sort(key=lambda x: x["started_at"], reverse=True)
    
    return {
        "total_jobs": len(jobs_list),
        "jobs": jobs_list
    }


@app.post("/run-pipeline/{database}", response_model=JobStatus, tags=["Pipeline"])
async def run_single_database(
    database: str,
    background_tasks: BackgroundTasks,
    output_dir: str = "data",
    forecast_type: str = "data",
    verbose: bool = True,
    upload_ftp: bool = False
    ):
    """
    Inicia a execução do pipeline ETL para um database específico em background.
    
    Args:
        database: Nome do database para executar as queries
        output_dir: Diretório base para salvar arquivos parquet (default: "data")
        forecast_type: Tipo de dados para FTP (default: "data")
        verbose: Exibir logs detalhados (default: True)
        upload_ftp: Fazer upload automático para FTP após extração (default: False)
        
    Returns:
        JobStatus com job_id e status inicial
    """
    # Gerar job ID único
    job_id = str(uuid.uuid4())
    
    # Criar entrada no storage de jobs
    jobs[job_id] = {
        "status": "pending",
        "started_at": datetime.now().isoformat(),
        "database": database,
        "output_dir": output_dir,
        "forecast_type": forecast_type,
        "verbose": verbose,
        "upload_ftp": upload_ftp,
        "sql_results": None,
        "ftp_results": None,
        "error": None,
        "completed_at": None
    }
    
    # Adicionar tarefa em background
    background_tasks.add_task(
        execute_single_database_task,
        job_id=job_id,
        database=database,
        output_dir=output_dir,
        forecast_type=forecast_type,
        verbose=verbose,
        upload_ftp=upload_ftp
    )
    
    return {
        "job_id": job_id,
        "status": "pending",
        "message": f"Pipeline ETL para database '{database}' iniciado em background",
        "started_at": jobs[job_id]["started_at"]
    }


@app.post("/upload-supabase/{database}", response_model=SupabaseUploadResponse, tags=["Supabase"])
async def upload_to_supabase(
    database: str,
    request: SupabaseUploadRequest,
    background_tasks: BackgroundTasks
    ):
    """
    Inicia o upload de arquivos Parquet de um database para Supabase em background.
    
    Args:
        database: Nome do database (diretório contendo os arquivos parquet)
        request: Parâmetros de configuração do upload
        background_tasks: Tarefas em background do FastAPI
        
    Returns:
        SupabaseUploadResponse com informações do job iniciado
    """
    # Determinar nome do bucket (usa database se não especificado)
    bucket_name = request.bucket_name or database.lower().replace("_", "-")
    
    # Gerar job ID único
    job_id = str(uuid.uuid4())
    
    # Criar entrada no storage de jobs
    jobs[job_id] = {
        "status": "pending",
        "started_at": datetime.now().isoformat(),
        "database": database,
        "bucket_name": bucket_name,
        "output_dir": request.output_dir,
        "supabase_results": None,
        "error": None,
        "completed_at": None
    }
    
    # Adicionar tarefa em background
    background_tasks.add_task(
        execute_supabase_upload_task,
        job_id=job_id,
        database=database,
        bucket_name=bucket_name,
        output_dir=request.output_dir
    )
    
    return {
        "job_id": job_id,
        "status": "pending",
        "message": f"Upload para Supabase do database '{database}' iniciado em background",
        "started_at": jobs[job_id]["started_at"],
        "database": database,
        "bucket_name": bucket_name
    }


@app.post("/run-supabase-pipeline/{database}", response_model=SupabasePipelineResponse, tags=["Supabase"])
async def run_supabase_pipeline(
    database: str,
    request: SupabasePipelineRequest,
    background_tasks: BackgroundTasks
):
    """
    Inicia a execução do pipeline ETL Supabase para um database específico em background.
    
    Args:
        database: Nome do database para executar as queries
        request: Parâmetros de configuração do pipeline
        background_tasks: Tarefas em background do FastAPI
        
    Returns:
        SupabasePipelineResponse com informações do job iniciado
    """
    # Determinar nome do bucket (usa database se não especificado)
    bucket_name = request.bucket_name or database.lower().replace("_", "-")
    
    # Gerar job ID único
    job_id = str(uuid.uuid4())
    
    # Criar entrada no storage de jobs
    jobs[job_id] = {
        "status": "pending",
        "started_at": datetime.now().isoformat(),
        "database": database,
        "bucket_name": bucket_name,
        "verbose": request.verbose,
        "temp_dir": request.temp_dir,
        "sql_results": None,
        "supabase_results": None,
        "error": None,
        "completed_at": None
    }
    
    # Adicionar tarefa em background
    background_tasks.add_task(
        execute_supabase_pipeline_task,
        job_id=job_id,
        database=database,
        bucket_name=bucket_name,
        verbose=request.verbose,
        temp_dir=request.temp_dir
    )
    
    return {
        "job_id": job_id,
        "status": "pending",
        "message": f"Pipeline ETL Supabase para database '{database}' iniciado em background",
        "started_at": jobs[job_id]["started_at"],
        "database": database,
        "bucket_name": bucket_name
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

