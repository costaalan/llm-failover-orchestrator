"""
FastAPI Backend - LLM Failover Orchestrator.
Endpoints: monitor real, injecao de falha, pipeline, websocket.
"""
import os
import sys
import json
import asyncio
import threading
import datetime
from pathlib import Path
from contextlib import asynccontextmanager

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest


class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []
    
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
    
    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
    
    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()
last_real_status = {"anthropic": {"status": "unknown"}, "openai": {"status": "unknown"}}

# Prometheus metrics
FAILOVER_FAILURES = Counter("llm_failover_failures_total", "Total simulated failures", ["provider"])
FAILOVER_RECOVERIES = Counter("llm_failover_recoveries_total", "Total recoveries", ["provider"])
FAILOVER_PIPELINE_DURATION = Histogram("llm_failover_pipeline_seconds", "Pipeline duration", buckets=[1, 5, 10, 30, 60])
FAILOVER_PROVIDER_STATUS = Gauge("llm_failover_provider_status", "Provider health 1=up 0=down", ["provider"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Iniciar monitor real em background
    async def poll_real_status():
        from services.real_status_monitor.monitor import check_all
        while True:
            try:
                result = check_all()
                global last_real_status
                last_real_status = result["providers"]
                await manager.broadcast({"event": "real_status_update", "data": result})
            except Exception as e:
                print(f"[MONITOR] Error: {e}")
            await asyncio.sleep(60)
    
    task = asyncio.create_task(poll_real_status())
    yield
    task.cancel()


app = FastAPI(title="LLM Failover Orchestrator", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Endpoints ---

@app.get("/health")
def health():
    # Update provider status gauges
    for provider, info in last_real_status.items():
        FAILOVER_PROVIDER_STATUS.labels(provider=provider).set(1 if info.get("status") == "ok" else 0)
    return {"status": "ok", "service": "llm-failover-orchestrator"}


@app.get("/metrics")
def metrics():
    # Update status gauges before returning
    for provider, info in last_real_status.items():
        FAILOVER_PROVIDER_STATUS.labels(provider=provider).set(1 if info.get("status") == "ok" else 0)
    return Response(content=generate_latest(), media_type="text/plain; charset=utf-8")


@app.get("/api/real-status")
def get_real_status():
    return {"providers": last_real_status}


@app.get("/api/projects")
def get_projects():
    cat_path = Path(__file__).parent.parent / "simulation" / "synthetic-projects" / "catalog.json"
    with open(cat_path) as f:
        projects = json.load(f)
    return {"projects": projects, "total": len(projects)}


class SimulateRequest(BaseModel):
    provider: str  # "anthropic" | "openai"

import threading
import queue

pipeline_results = {}
pipeline_queue = queue.Queue()

def _run_pipeline_async(provider):
    """Roda pipeline em background e guarda resultado."""
    import sys, traceback, time
    t0 = time.time()
    sys.path.insert(0, '/opt/data/llm-failover-orchestrator')
    from orchestrator.engine import run_pipeline_with_guardrails
    from orchestrator.guardrails import CircuitBreakerOpenError
    try:
        result = run_pipeline_with_guardrails("simulated_injection", provider)
        FAILOVER_FAILURES.labels(provider=provider).inc()
        FAILOVER_PIPELINE_DURATION.observe(time.time() - t0)
        pipeline_results[provider] = {"status": "completed", **result}
        print(f"[ASYNC] Pipeline completed for {provider}: {len(result.get('projects_affected',[]))} projetos")
    except CircuitBreakerOpenError as e:
        traceback.print_exc()
        pipeline_results[provider] = {"status": "circuit_breaker_open", "error": str(e)}
        print(f"[ASYNC] Circuit breaker OPEN for {provider}: {e}")
    except Exception as e:
        traceback.print_exc()
        FAILOVER_FAILURES.labels(provider=provider).inc()
        pipeline_results[provider] = {"status": "error", "error": str(e)}
        print(f"[ASYNC] Pipeline error for {provider}: {e}")


@app.post("/api/simulate-failure")
def simulate_failure(req: SimulateRequest):
    """Dispara pipeline assincrona e retorna imediatamente."""
    provider = req.provider
    pipeline_results[provider] = {"status": "processing"}
    
    # Reset circuit breaker para permitir nova tentativa
    import sys
    sys.path.insert(0, '/opt/data/llm-failover-orchestrator')
    from orchestrator.guardrails import get_circuit_breaker
    get_circuit_breaker().record_success(provider)
    
    t = threading.Thread(target=_run_pipeline_async, args=(provider,), daemon=True)
    t.start()
    return {"status": "processing", "provider": provider}


@app.get("/api/simulate")
def simulate_failure_get():
    """Inicia pipeline e aguarda resultado."""
    provider = "anthropic"
    import sys, traceback
    sys.path.insert(0, '/opt/data/llm-failover-orchestrator')
    from orchestrator.engine import run_pipeline_with_guardrails
    from orchestrator.guardrails import CircuitBreakerOpenError
    try:
        result = run_pipeline_with_guardrails("simulated_injection", provider)
        pipeline_results[provider] = {"status": "completed", **result}
        return {"status": "completed", **result}
    except CircuitBreakerOpenError as e:
        traceback.print_exc()
        pipeline_results[provider] = {"status": "circuit_breaker_open", "error": str(e)}
        return {"status": "circuit_breaker_open", "error": str(e)}
    except Exception as e:
        traceback.print_exc()
        pipeline_results[provider] = {"status": "error", "error": str(e)}
        return {"status": "error", "error": str(e)}


@app.get("/api/pipeline-status")
def pipeline_status():
    provider = "anthropic"
    result = pipeline_results.get(provider, {"status": "not_started"})
    return result


# --- WebSocket ---

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        # Enviar status inicial
        await ws.send_json({"event": "real_status_update", "data": {"providers": last_real_status}})
        
        while True:
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("command") == "ping":
                    await ws.send_json({"event": "pong"})
            except:
                pass
    except WebSocketDisconnect:
        manager.disconnect(ws)


# Frontend estatico
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
