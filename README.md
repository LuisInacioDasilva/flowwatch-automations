# Flowwatch Automations

Automação boa não é só um cron que roda quando dá certo. Este projeto é um motor local para executar rotinas com fila persistente, agendas recorrentes, chave de idempotência, retries, lease de execução e trilha por tentativa. Fiz o exemplo com tarefas sintéticas de normalização de contato e resumo de valores; os handlers não enviam nada para fora.

## Ver rodando

Requisito: Python 3.10+. Sem pacote pago, Docker ou conta externa.

```bash
python -m unittest discover -s tests -v
python -m flowwatch demo
```

Abra `dist/index.html` para ver o relatório responsivo de jobs, tentativas e agendas. A demonstração usa SQLite em memória e dados falsos. Ela inclui um job inválido para mostrar backoff e fila morta, não apenas o caminho feliz.

## Uso como worker local

```bash
python -m flowwatch enqueue --key contato-001 --type normalize_contact --payload '{"name":"Ana","phone":"+55 11 99999-1234"}'
python -m flowwatch run-once
python -m flowwatch report
python -m flowwatch worker --interval 2
python -m flowwatch metrics
```

No PowerShell, as aspas do JSON podem exigir escape diferente; o comando `demo` dispensa isso. O banco padrão fica em `data/flowwatch.sqlite`, ignorado pelo Git. `worker` é um processo contínuo que pode ser parado com Ctrl+C. Em produção, executaria esse processo sob um supervisor e monitoraria o número de jobs `dead` e a idade da fila `pending`.

## Agendas recorrentes

```bash
python -m flowwatch schedule-add --name resumo-horario --type draft_summary --payload '{"amounts_cents":[100,250]}' --interval 3600
python -m flowwatch schedule-tick
python -m flowwatch run-once
python -m flowwatch schedule-toggle --id 1 --enabled no
```

`worker` e `run-once` materializam ocorrências vencidas antes de processar. Cada horário devido vira um job com chave única `schedule:<id>:<timestamp>` e um registro imutável em `schedule_runs`. Duas instâncias no mesmo arquivo SQLite serializam o tick com `BEGIN IMMEDIATE`; repetir o tick não cria outra execução para o mesmo horário. O catch-up cria no máximo 10 ocorrências por agenda por ciclo para não inundar a fila após uma parada longa; as restantes permanecem vencidas e entram nos ciclos seguintes. Pausar não apaga histórico nem adianta o próximo horário. Um nome de agenda reaproveitado com configuração diferente gera conflito explícito.

## Recuperação operacional

Um job na fila morta não é reativado silenciosamente. Depois de corrigir a causa na origem, o operador pode executar `python -m flowwatch requeue-dead --job-id 3 --actor equipe-ti --reason "Falha transitória corrigida na origem"`. Isso cria **outro job**, mantém o original e sua chave de idempotência intactos e grava uma ação de operador imutável ligando ambos. Não edite o payload diretamente no banco; se ele estiver incorreto, crie um novo job com chave e conteúdo próprios.

## Por que essas peças existem

`idempotency_key` tem `UNIQUE` no schema; mesma chave e mesmo conteúdo reutilizam o job, conteúdo diferente falha. Enfileiramento e claim usam `BEGIN IMMEDIATE` para serializar concorrência de escritores no SQLite. O worker recebe um lease com token; se morrer, outro worker recupera o job após o vencimento. O token impede que o worker antigo finalize um job já recuperado. Cada tentativa registra início, resultado e erro; o backoff exponencial é limitado a cinco minutos, e o job vai para `dead` ao esgotar tentativas. Resultado e estado final são gravados na mesma transação.

O projeto usa SQLite por ser simples de executar e inspecionar. Para múltiplos workers em escala, migraria a fila para PostgreSQL ou um broker dedicado, preservando as mesmas invariantes e adicionando testes de concorrência reais. Não há tentativa de vender SQLite como solução universal para alta vazão.

## Testes, deploy e rollback

Os testes cobrem replay, conflito de payload, execução atômica, retries, fila morta, lease expirado, proteção contra worker obsoleto, agendas, catch-up limitado, escape no relatório, constraints e rollback. `flowwatch/up.sql` adiciona as tabelas de agenda em bases já existentes com `CREATE TABLE IF NOT EXISTS`; faça backup antes de instalar a nova versão. `flowwatch/down.sql` desfaz todo o esquema, **apagando agendas, jobs, resultados e tentativas**. Em uma instância com dados reais, pause os workers, copie o arquivo SQLite e teste a restauração antes de executar o rollback. Não execute o comando `demo` com dados reais: ele foi criado só para portfólio.

---

<p align="center"><a href="https://github.com/LuisInacioDasilva"><img src="https://avatars.githubusercontent.com/u/88007376?v=4" width="82" height="82" alt="Foto de Luis da Silva" style="border-radius:50%"></a></p>
<p align="center"><strong>Desenvolvido por Luis da Silva</strong><br>Back-end, dados, integrações e automação em produção.</p>
