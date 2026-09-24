"""Mede vazão e custo dos runs recentes dos jobs Glue (laboratório de escala, docs/labs_aws.md).

Lê o histórico de runs pela AWS CLI (sem boto3, como o resto do projeto), agrupa por
configuração de worker e por dia (--dt) e converte em minutos por job, DPU-hora,
custo em US$ e, dado o volume do dia, linhas/s e linhas/s por vCPU: o número que
falta para extrapolar ao volume de produção. ExecutionTime é o tempo cobrado pelo
Glue (exclui provisionamento); a cobrança mínima por run é de 1 minuto.

Uso:
    python3 scripts/medir_vazao_glue.py --ultimos 3 --linhas-por-dia 666660
    python3 scripts/medir_vazao_glue.py --prefixo saldo-contrato --regiao us-east-1 --meta 300000000
"""
from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict

JOBS = ("bronze_ingest", "silver_quality", "gold_saldo")
DPU_POR_WORKER = {"G.025X": 0.25, "G.1X": 1, "G.2X": 2, "G.4X": 4, "G.8X": 8}
VCPU_POR_WORKER = {"G.025X": 2, "G.1X": 4, "G.2X": 8, "G.4X": 16, "G.8X": 32}
VCPU_ALVO = {"20 × G.2X": 160, "10 × G.4X": 160, "40 × G.2X": 320}


def fmt(n: float, casas: int = 0) -> str:
    return f"{n:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def job_runs(job: str, ultimos: int, regiao: str | None) -> list[dict]:
    cmd = ["aws", "glue", "get-job-runs", "--job-name", job, "--max-results", str(ultimos)]
    cmd += ["--output", "json"]
    if regiao:
        cmd += ["--region", regiao]
    saida = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    return json.loads(saida)["JobRuns"]


def medir(run: dict, preco_dpu_hora: float) -> dict:
    worker = run.get("WorkerType", "?")
    workers = int(run.get("NumberOfWorkers") or 0)
    dpu = float(run.get("MaxCapacity") or DPU_POR_WORKER.get(worker, 0) * workers)
    exec_s = int(run.get("ExecutionTime") or 0)
    dpu_h = float(run["DPUSeconds"]) / 3600 if run.get("DPUSeconds") else dpu * exec_s / 3600
    return {
        "id": run["Id"][:12],
        "inicio": str(run.get("StartedOn", ""))[:16],
        "estado": run.get("JobRunState", "?"),
        "dt": (run.get("Arguments") or {}).get("--dt", "-"),
        "config": f"{workers} × {worker}",
        "vcpu": workers * VCPU_POR_WORKER.get(worker, 0),
        "exec_s": exec_s,
        "dpu_h": dpu_h,
        "usd": dpu_h * preco_dpu_hora,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Vazão e custo dos runs Glue")
    parser.add_argument("--prefixo", default="saldo-contrato")
    parser.add_argument("--ultimos", type=int, default=3, help="runs por job a considerar")
    parser.add_argument("--regiao", default=None)
    parser.add_argument("--preco-dpu-hora", type=float, default=0.44, help="US$/DPU-h (us-east-1)")
    parser.add_argument(
        "--linhas-por-dia", type=int, default=None, help="linhas de cada dia do dataset medido"
    )
    parser.add_argument("--meta", type=int, default=300_000_000, help="volume diário alvo da extrapolação")
    parser.add_argument("--todos", action="store_true", help="inclui runs que não terminaram em SUCCEEDED")
    args = parser.parse_args()

    por_dia: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)  # (config, dt) -> job -> medida
    for job in JOBS:
        nome = f"{args.prefixo}-{job}"
        print(f"\n=== {nome} (últimos {args.ultimos} runs) ===")
        print(f"{'run':<13}{'início':<18}{'estado':<10}{'dt':<12}{'config':<12}{'exec':>8}{'DPU-h':>8}{'US$':>7}")
        for run in job_runs(nome, args.ultimos, args.regiao):
            m = medir(run, args.preco_dpu_hora)
            print(f"{m['id']:<13}{m['inicio']:<18}{m['estado']:<10}{m['dt']:<12}{m['config']:<12}"
                  f"{m['exec_s'] / 60:>7.1f}m{m['dpu_h']:>8.3f}{m['usd']:>7.2f}")
            if (m["estado"] == "SUCCEEDED" or args.todos) and job not in por_dia[(m["config"], m["dt"])]:
                por_dia[(m["config"], m["dt"])][job] = m  # o run mais recente de cada job por dia

    completos = {chave: jobs for chave, jobs in por_dia.items() if len(jobs) == len(JOBS)}
    if not completos:
        print("\nnenhum dia com os três jobs concluídos na janela; aumente --ultimos ou use --todos")
        return

    print("\n=== Pipeline completo por dia (soma dos três jobs) ===")
    por_config: dict[str, list[dict]] = defaultdict(list)
    for (config, dt), jobs in sorted(completos.items()):
        total = {
            "exec_s": sum(m["exec_s"] for m in jobs.values()),
            "dpu_h": sum(m["dpu_h"] for m in jobs.values()),
            "usd": sum(m["usd"] for m in jobs.values()),
            "vcpu": next(iter(jobs.values()))["vcpu"],
        }
        por_config[config].append(total)
        detalhe = " + ".join(f"{j.split('_')[0]} {m['exec_s'] / 60:.1f}m" for j, m in jobs.items())
        print(f"{config:<12}{dt:<12}{total['exec_s'] / 60:>6.1f} min  DPU-h {total['dpu_h']:.3f}  "
              f"US$ {total['usd']:.2f}   ({detalhe})")

    print("\n=== Média por configuração ===")
    for config, totais in por_config.items():
        n = len(totais)
        exec_s = sum(t["exec_s"] for t in totais) / n
        usd = sum(t["usd"] for t in totais) / n
        vcpu = totais[0]["vcpu"]
        linha = f"{config:<12}{n} dia(s)  {exec_s / 60:>6.1f} min/execução  US$ {usd:.2f}/execução"
        linha += f"  {vcpu} vCPU"
        if args.linhas_por_dia:
            vazao = args.linhas_por_dia / exec_s
            por_vcpu = vazao / vcpu if vcpu else 0
            linha += f"  → {fmt(vazao)} linhas/s · {fmt(por_vcpu)} linhas/s/vCPU"
            print(linha)
            print(f"    extrapolação para {fmt(args.meta)} linhas/dia (escala linear ideal):")
            print(f"      mesma configuração: {args.meta / vazao / 3600:.1f} h")
            for alvo, vcpu_alvo in VCPU_ALVO.items():
                if por_vcpu:
                    minutos = args.meta / (por_vcpu * vcpu_alvo) / 60
                    print(f"      {alvo} ({vcpu_alvo} vCPU): {minutos:.0f} min")
        else:
            print(linha + "  (passe --linhas-por-dia para vazão e extrapolação)")


if __name__ == "__main__":
    main()
