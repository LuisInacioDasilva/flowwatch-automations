import html
from datetime import datetime, timezone
from pathlib import Path


def write_report(store, path: str | Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = {row["state"]: row["count"] for row in store.db.execute("SELECT state, count(*) AS count FROM jobs GROUP BY state")}
    jobs = list(store.db.execute("""SELECT id, task_type, state, attempts, max_attempts, last_error
        FROM jobs ORDER BY id DESC"""))
    attempts = list(store.db.execute("""SELECT a.job_id, a.attempt_number, a.outcome, a.error
        FROM attempt_log a ORDER BY a.id DESC LIMIT 20"""))
    schedules = list(store.db.execute("""SELECT s.id, s.name, s.interval_seconds, s.next_run_at, s.enabled,
        count(r.id) AS runs FROM schedules s LEFT JOIN schedule_runs r ON r.schedule_id = s.id
        GROUP BY s.id ORDER BY s.id"""))
    e = html.escape
    table_rows = "".join(
        f"<tr><td>#{job['id']}</td><td>{e(job['task_type'])}</td><td><span class='pill {e(job['state'])}'>{e(job['state'])}</span></td>"
        f"<td>{job['attempts']} / {job['max_attempts']}</td><td>{e(job['last_error'] or '—')}</td></tr>"
        for job in jobs
    )
    attempt_rows = "".join(
        f"<tr><td>#{row['job_id']}</td><td>{row['attempt_number']}</td><td>{e(row['outcome'] or 'running')}</td><td>{e(row['error'] or '—')}</td></tr>"
        for row in attempts
    )
    schedule_rows = "".join(
        f"<tr><td>#{row['id']}</td><td>{e(row['name'])}</td><td>{row['interval_seconds']} s</td>"
        f"<td>{e(datetime.fromtimestamp(row['next_run_at'], timezone.utc).strftime('%d/%m/%Y %H:%M UTC'))}</td>"
        f"<td>{'ativa' if row['enabled'] else 'pausada'}</td><td>{row['runs']}</td></tr>"
        for row in schedules
    )
    document = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Flowwatch · Execuções</title><style>
    :root{{font-family:Inter,ui-sans-serif,system-ui,sans-serif;color-scheme:dark;background:#090d17;color:#edf3fd}}*{{box-sizing:border-box}}
    body{{margin:0;background:radial-gradient(circle at 78% 0,#283054 0,transparent 40%),#090d17;min-height:100vh}}main{{max-width:1120px;margin:auto;padding:52px 24px 80px}}
    .eyebrow{{letter-spacing:.16em;text-transform:uppercase;color:#a7a0ff;font-size:12px;font-weight:800}}h1{{font-size:clamp(40px,5vw,64px);letter-spacing:-.06em;margin:22px 0 10px}}p{{color:#a8b6cc;max-width:680px;line-height:1.6}}
    .stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:32px 0}}.stat,.panel{{background:#141c2d;border:1px solid #34405b;border-radius:16px}}
    .stat{{padding:22px}}.stat span{{font-size:11px;letter-spacing:.1em;color:#9aaecb;text-transform:uppercase}}.stat strong{{display:block;font-size:35px;margin-top:8px}}
    .panel{{padding:23px;margin:17px 0;overflow:auto}}h2{{margin:0 0 18px;font-size:19px}}table{{width:100%;border-collapse:collapse;min-width:550px}}td,th{{padding:14px 10px;text-align:left;border-bottom:1px solid #303d56;font-size:13px}}
    th{{color:#8e9fb9;font-size:11px;text-transform:uppercase;letter-spacing:.09em}}tr:last-child td{{border:0}}.pill{{padding:5px 9px;border-radius:100px;background:#2f3d5f;font-weight:700;font-size:11px}}
    .done{{background:#194b3e;color:#82e8bb}}.dead{{background:#532838;color:#ffa0b7}}.running{{background:#504022;color:#f9d48a}}footer{{color:#7b8ca7;font-size:12px;margin-top:20px}}
    @media(max-width:700px){{.stats{{grid-template-columns:repeat(2,1fr)}}}}
    </style></head><body><main><div class="eyebrow">FLOWWATCH / AUTOMATION OPERATIONS</div><h1>Automação sem caixa-preta.</h1>
    <p>Fila durável, agendas recorrentes, chave de idempotência, tentativas com backoff, leases e trilha de execução. O relatório é gerado a partir do banco local, sem chamadas externas.</p>
    <section class="stats"><div class="stat"><span>Pendentes</span><strong>{counts.get('pending', 0)}</strong></div><div class="stat"><span>Em execução</span><strong>{counts.get('running', 0)}</strong></div><div class="stat"><span>Concluídos</span><strong>{counts.get('done', 0)}</strong></div><div class="stat"><span>Fila morta</span><strong>{counts.get('dead', 0)}</strong></div></section>
    <section class="panel"><h2>Jobs</h2><table><thead><tr><th>ID</th><th>Rotina</th><th>Estado</th><th>Tentativas</th><th>Último erro</th></tr></thead><tbody>{table_rows}</tbody></table></section>
    <section class="panel"><h2>Últimas tentativas</h2><table><thead><tr><th>Job</th><th>Tentativa</th><th>Resultado</th><th>Erro</th></tr></thead><tbody>{attempt_rows}</tbody></table></section>
    <section class="panel"><h2>Agendas</h2><table><thead><tr><th>ID</th><th>Nome</th><th>Intervalo</th><th>Próxima execução</th><th>Estado</th><th>Ocorrências</th></tr></thead><tbody>{schedule_rows}</tbody></table></section>
    <footer>Dados sintéticos na demo. Nenhum telefone ou payload é exibido no relatório.<div style="display:flex;align-items:center;gap:12px;margin-top:22px;padding-top:19px;border-top:1px solid #34405b"><img src="https://avatars.githubusercontent.com/u/88007376?v=4" alt="Foto de Luis da Silva" width="44" height="44" style="border-radius:50%;object-fit:cover"><span>Desenvolvido por <a href="https://github.com/LuisInacioDasilva" style="color:#a7a0ff">Luis da Silva</a> · Back-end e automações.</span></div></footer></main></body></html>"""
    path.write_text(document, encoding="utf-8")
    return path
