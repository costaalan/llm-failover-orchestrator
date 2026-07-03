"""
Real Status Monitor - Consulta o status verdadeiro dos provedores de LLM.
Nunca simula resultados. Independente do modo de demonstracao.
"""
import json
import time
import urllib.request
import urllib.error
import datetime

PROVIDERS = {
    "anthropic": {
        "status_url": "https://status.anthropic.com/api/v2/status.json",
        "incidents_url": "https://status.anthropic.com/api/v2/incidents.json",
        "health_check": None,
    },
    "openai": {
        "status_url": "https://status.openai.com/api/v2/status.json",
        "incidents_url": "https://status.openai.com/api/v2/incidents.json",
        "health_check": None,
    },
}


def check_provider_status(provider: str) -> dict:
    """Consulta o status real de um provedor via API de status page."""
    info = PROVIDERS[provider]
    result = {
        "provider": provider,
        "source": "real_monitor",
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "unknown",
        "description": "",
        "incidents": [],
        "error": None,
    }

    # Status page
    try:
        req = urllib.request.Request(info["status_url"], headers={"User-Agent": "LLM-Failover-Orchestrator/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if provider == "anthropic":
                result["status"] = data.get("status", {}).get("indicator", "unknown")
                result["description"] = data.get("status", {}).get("description", "")
            else:
                result["status"] = data.get("status", {}).get("indicator", "unknown")
                result["description"] = data.get("status", {}).get("description", "")
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    # Incidents recentes
    try:
        req = urllib.request.Request(info["incidents_url"], headers={"User-Agent": "LLM-Failover-Orchestrator/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            incidents = data.get("incidents", [])
            result["incidents"] = [
                {"name": inc.get("name"), "status": inc.get("status"),
                 "created_at": inc.get("created_at"), "impact": inc.get("impact")}
                for inc in incidents[:5]
            ]
    except:
        pass

    # Normalizar status
    if result["status"] in ["none", "operational"]:
        result["status"] = "operational"
    elif result["status"] in ["minor", "degraded_performance"]:
        result["status"] = "degraded"
    elif result["status"] in ["major", "partial_outage"]:
        result["status"] = "major_outage"
    elif result["status"] in ["critical", "complete_outage"]:
        result["status"] = "outage"

    return result


def check_all(timeout_seconds: int = 15) -> dict:
    """Checa todos os provedores e retorna resultado consolidado."""
    results = {}
    for provider in PROVIDERS:
        results[provider] = check_provider_status(provider)
    
    consolidated = {
        "source": "real_monitor",
        "timestamp": datetime.datetime.now().isoformat(),
        "providers": results,
        "all_operational": all(r["status"] == "operational" for r in results.values()),
        "any_outage": any(r["status"] in ["major_outage", "outage"] for r in results.values()),
    }
    return consolidated


if __name__ == "__main__":
    print("=== Real Status Monitor ===")
    result = check_all()
    for provider, status in result["providers"].items():
        print(f"\n{provider.upper()}:")
        print(f"  Status: {status['status']}")
        print(f"  Descricao: {status['description']}")
        if status["incidents"]:
            print(f"  Incidentes recentes: {len(status['incidents'])}")
            for inc in status["incidents"][:2]:
                print(f"    - {inc['name']} ({inc['impact']})")
        if status["error"]:
            print(f"  Erro: {status['error']}")
