"""Logging estruturado em JSON (uma linha por evento).

No Glue, stdout vai para o CloudWatch Logs; linhas JSON permitem métricas e filtros
(ex.: CloudWatch Logs Insights: `filter evento="gate_reprovado"`). Localmente, as
mesmas linhas são legíveis com `jq`.
"""
import json
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone


class JobLogger:
    def __init__(self, job: str):
        self.job = job

    def evento(self, evento: str, **campos) -> None:
        registro = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "job": self.job,
            "evento": evento,
            **campos,
        }
        print(json.dumps(registro, ensure_ascii=False, default=str), file=sys.stdout, flush=True)

    @contextmanager
    def etapa(self, nome: str, **campos):
        """Mede duração e registra sucesso/falha da etapa; a exceção segue propagando."""
        inicio = time.monotonic()
        self.evento("etapa_inicio", etapa=nome, **campos)
        try:
            yield
        except Exception as exc:
            self.evento(
                "etapa_erro",
                etapa=nome,
                duracao_s=round(time.monotonic() - inicio, 3),
                erro=type(exc).__name__,
                mensagem=str(exc),
                **campos,
            )
            raise
        self.evento("etapa_fim", etapa=nome, duracao_s=round(time.monotonic() - inicio, 3), **campos)
