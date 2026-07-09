"""
Guardrails para LLM Failover Orchestrator.

Implementa:
- Timeout de 30s por tentativa
- Retry com exponential backoff (3 tentativas: 1s, 2s, 4s)
- Circuit Breaker: 3 falhas consecutivas → pausa de 60s por provider
"""
import time
import threading
from typing import Any, Callable


class CircuitBreaker:
    """Circuit breaker simples: 3 falhas consecutivas → pausa de 60s."""

    def __init__(self, failure_threshold: int = 3, cooldown_seconds: float = 60.0):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._failures: dict[str, int] = {}
        self._last_failure_time: dict[str, float] = {}
        self._lock = threading.Lock()

    def record_failure(self, provider: str) -> None:
        """Registra uma falha para o provider."""
        with self._lock:
            self._failures[provider] = self._failures.get(provider, 0) + 1
            self._last_failure_time[provider] = time.time()

    def record_success(self, provider: str) -> None:
        """Reseta o contador de falhas para o provider."""
        with self._lock:
            self._failures[provider] = 0
            self._last_failure_time.pop(provider, None)

    def is_open(self, provider: str) -> bool:
        """
        Retorna True se o circuit breaker está aberto (recusando chamadas)
        para o provider.
        """
        with self._lock:
            failures = self._failures.get(provider, 0)
            if failures >= self.failure_threshold:
                last_ts = self._last_failure_time.get(provider, 0)
                elapsed = time.time() - last_ts
                if elapsed < self.cooldown_seconds:
                    return True
                # Cooldown expirado, reseta
                self._failures[provider] = 0
                self._last_failure_time.pop(provider, None)
            return False

    def time_until_reset(self, provider: str) -> float:
        """Tempo restante (em segundos) até o circuit breaker resetar."""
        last_ts = self._last_failure_time.get(provider, 0)
        elapsed = time.time() - last_ts
        remaining = self.cooldown_seconds - elapsed
        return max(0.0, remaining)

    def get_failure_count(self, provider: str) -> int:
        """Retorna o número atual de falhas consecutivas."""
        with self._lock:
            return self._failures.get(provider, 0)


class CircuitBreakerOpenError(Exception):
    """Exceção levantada quando o circuit breaker está aberto."""
    pass


# Instância global do circuit breaker
_circuit_breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=60.0)


def get_circuit_breaker() -> CircuitBreaker:
    """Retorna a instância global do circuit breaker."""
    return _circuit_breaker


def run_with_retry_and_timeout(
    func: Callable,
    *args: Any,
    max_retries: int = 3,
    timeout_seconds: float = 30.0,
    provider: str = "unknown",
    **kwargs: Any,
) -> Any:
    """
    Executa uma função com timeout, retry com exponential backoff, e circuit breaker.

    Args:
        func: Função a ser executada.
        *args: Argumentos posicionais para func.
        max_retries: Número máximo de tentativas (default: 3).
        timeout_seconds: Timeout por tentativa em segundos (default: 30).
        provider: Nome do provider para circuit breaker.
        **kwargs: Argumentos nomeados para func.

    Returns:
        O resultado da função.

    Raises:
        CircuitBreakerOpenError: Se o circuit breaker estiver aberto.
        TimeoutError: Se a função exceder o timeout em todas as tentativas.
        Exception: A última exceção se todas as tentativas falharem.
    """
    cb = get_circuit_breaker()

    # Verifica circuit breaker antes de qualquer tentativa
    if cb.is_open(provider):
        remaining = cb.time_until_reset(provider)
        raise CircuitBreakerOpenError(
            f"Circuit breaker OPEN para provider '{provider}'. "
            f"Tente novamente em {remaining:.0f}s."
        )

    last_exception: Exception | None = None

    for attempt in range(max_retries):
        try:
            # Executa func em thread separada para poder aplicar timeout
            result_container: dict[str, Any] = {}
            exception_container: dict[str, Exception] = {}

            def _target() -> None:
                try:
                    result_container["result"] = func(*args, **kwargs)
                except Exception as e:
                    exception_container["exc"] = e

            thread = threading.Thread(target=_target, daemon=True)
            thread.start()
            thread.join(timeout=timeout_seconds)

            if thread.is_alive():
                # Timeout: a thread ainda está rodando
                raise TimeoutError(
                    f"Timeout após {timeout_seconds}s na tentativa "
                    f"{attempt + 1}/{max_retries}"
                )

            if "exc" in exception_container:
                raise exception_container["exc"]

            # Sucesso!
            cb.record_success(provider)
            return result_container["result"]

        except CircuitBreakerOpenError:
            raise

        except Exception as e:
            last_exception = e
            cb.record_failure(provider)

            # Verifica se o circuit breaker abriu após esta falha
            if cb.is_open(provider):
                remaining = cb.time_until_reset(provider)
                raise CircuitBreakerOpenError(
                    f"Circuit breaker OPEN para provider '{provider}' após "
                    f"{cb.failure_threshold} falhas consecutivas. "
                    f"Tente novamente em {remaining:.0f}s."
                ) from e

            if attempt < max_retries - 1:
                wait = (2 ** attempt) * 1.0  # 1s, 2s, 4s
                print(
                    f"[GUARDRAILS] Tentativa {attempt + 1}/{max_retries} falhou "
                    f"({type(e).__name__}: {e}). Retry em {wait:.0f}s..."
                )
                time.sleep(wait)
            else:
                print(
                    f"[GUARDRAILS] Todas as {max_retries} tentativas falharam "
                    f"para provider '{provider}'."
                )

    # Só chega aqui se todas as tentativas falharam sem abrir o circuit breaker
    raise last_exception  # type: ignore[misc]
