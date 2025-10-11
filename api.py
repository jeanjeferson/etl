"""
FastAPI ETL Pipeline Runner
API para executar pipeline ETL de forma assíncrona
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

from run_sql import run_etl_pipeline, upload_to_ftp


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
            "forecast_type": job_data.get("forecast_type")
        })
    
    # Ordenar por data de início (mais recente primeiro)
    jobs_list.sort(key=lambda x: x["started_at"], reverse=True)
    
    return {
        "total_jobs": len(jobs_list),
        "jobs": jobs_list
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

