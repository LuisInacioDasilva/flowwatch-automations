# Flowwatch Automations

Automação boa não é só um cron que roda quando dá certo. Este projeto é um motor pequeno para executar rotinas locais com fila persistente, chave de idempotência, retries, lease de execução e trilha por tentativa. Fiz o exemplo com tarefas sintéticas de normalização de contato e resumo de valores; os handlers não enviam nada para fora.

## Ver rodando

Requisito: Python 3.10+. Sem pacote pago, Docker ou conta externa.

```bash
python -m unittest discover -s tests -v
python -m flowwatch demo
```

Abra `dist/index.html` para ver o relatório responsivo de jobs e tentativas. A demonstração usa SQLite em memória e dados falsos. Ela inclui um job inválido para mostrar backoff e fila morta, não apenas o caminho feliz.

## Uso como worker local

```bash
python -m flowwatch enqueue --key contato-001 --type normalize_contact --payload '{"name":"Ana","phone":"+55 11 99999-1234"}'
python -m flowwatch run-once
python -m flowwatch report
python -m flowwatch worker --interval 2
```

No PowerShell, as aspas do JSON podem exigir escape diferente; o comando `demo` dispensa isso. O banco padrão fica em `data/flowwatch.sqlite`, ignorado pelo Git. `worker` é um processo contínuo que pode ser parado com Ctrl+C. Em produção, executaria esse processo sob um supervisor e monitoraria o número de jobs `dead` e a idade da fila `pending`.

## Por que essas peças existem

`idempotency_key` tem `UNIQUE` no schema; mesma chave e mesmo conteúdo reutilizam o job, conteúdo diferente falha. Enfileiramento e claim usam `BEGIN IMMEDIATE` para serializar concorrência de escritores no SQLite. O worker recebe um lease com token; se morrer, outro worker recupera o job após o vencimento. O token impede que o worker antigo finalize um job já recuperado. Cada tentativa registra início, resultado e erro; o backoff exponencial é limitado a cinco minutos, e o job vai para `dead` ao esgotar tentativas. Resultado e estado final são gravados na mesma transação.

O projeto usa SQLite por ser simples de executar e inspecionar. Para múltiplos workers em escala, migraria a fila para PostgreSQL ou um broker dedicado, preservando as mesmas invariantes e adicionando testes de concorrência reais. Não há tentativa de vender SQLite como solução universal para alta vazão.

## Testes, deploy e rollback

Os testes cobrem replay, conflito de payload, execução atômica, retries, fila morta, lease expirado, proteção contra worker obsoleto, escape no relatório, constraints e rollback. A migração está em `flowwatch/up.sql`; `flowwatch/down.sql` a desfaz, **apagando jobs, resultados e tentativas**. Em uma instância com dados reais, pause os workers, copie o arquivo SQLite e teste a restauração antes de executar o rollback. Não execute o comando `demo` com dados reais: ele foi criado só para portfólio.
