# 🔁 LLM Failover Orchestrator

Sistema multiagente de resiliencia para provedores de LLM (Anthropic e OpenAI).
Detecta queda real de um provedor, avalia o impacto sobre projetos que dependem dele,
propoe fallback com IA local (Ollama), e apresenta tudo para aprovacao — com pipeline
animado mostrando o raciocinio de cada agente em tempo real.

## 🔍 Visao Geral

Quando um provedor de LLM cai (Anthropic ou OpenAI), diversos fluxos de negocios
param. Este sistema orquestra 7 agentes especialistas para:

1. **Monitor Real** — consulta as status pages da Anthropic e OpenAI de verdade
2. **Impact Mapping** — cruza o provedor caido com 15+ projetos ficticios
3. **Risk Analysis** — analisa cada projeto com IA local (Ollama)
4. **Human Approval** — simula a decisao que um humano tomaria
5. **Fallback Execution** — migra projetos aprovados para modelo local
6. **Restoration** — reverte quando o provedor volta
7. **Audit** — gera relatorio de divergencia e metricas

## 🧪 Simulacao vs Real

| Componente | Real | Simulado |
|---|---|---|
| Monitor de status | ✅ Consulta status.anthropic.com e status.openai.com | — |
| Injecao de falha | — | ✅ Visitante escolhe qual provedor "caiu" |
| Catalogo de projetos | — | ✅ 15 projetos ficticios (n8n, LangGraph, LangChain, Langflow, Agno) |
| Decisao humana | — | ✅ simulated-human-approver |

O monitor real roda em background o tempo todo. A injecao de falha e apenas
para demonstracao — o visitante ve o pipeline reagir como se fosse uma queda real.

## 🏗️ Stack

- **Orquestrador:** LangGraph (Python)
- **Backend:** FastAPI + WebSocket
- **LLM local:** Ollama (llama3.1:8b / qwen2.5:7b)
- **Frontend:** HTML + CSS + JS (puro, sem framework)
- **Persistencia:** JSON / SQLite

## 🚀 Como Rodar

**Pre-requisitos:**
- Python 3.11+
- Ollama rodando com `ollama pull qwen2.5:7b` (ou `llama3.1:8b`)

```bash
git clone https://github.com/costaalan/llm-failover-orchestrator.git
cd llm-failover-orchestrator

# Criar venv e instalar
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt

# Copiar env e configurar
cp .env.example .env
# Edite OLLAMA_HOST e OLLAMA_MODEL conforme necessario

# Rodar
source .venv/bin/activate
python -m backend.main
```

Acesse em **http://localhost:8702**

## 📁 Estrutura

```
/services/real-status-monitor/  # Monitor real (nunca simula)
/orchestrator/                  # LangGraph — 7 agentes
/simulation/failure-injector/   # Gatilho de falha (demo)
/simulation/synthetic-projects/ # Catalogo ficticio (15 projetos)
/simulation/simulated-human-approver/  # Agente de decisao simulada
/frontend/                      # Interface animada
/docs/                          # Documentacao
```

## 🔗 Links

- **Demo ao vivo:** https://alancosta.dev/llm-failover
- **Repositorio:** https://github.com/costaalan/llm-failover-orchestrator
