# Como executar

> **Integrando a biblioteca?** Veja o [Guia de Integração](docs/INTEGRACAO.md).

Requisitos: .NET SDK 10 e Docker.

## Subir a demo

```sh
docker compose up -d postgres
dotnet run --project src/BackgroundJobs.Demo
```

| Variável       | Padrão                                                                       |
| -------------- | ---------------------------------------------------------------------------- |
| `PORT`         | `8080`                                                                       |
| `WORKER_COUNT` | `4`                                                                          |
| `DATABASE_URL` | `Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs` (também aceita `postgres://user:pass@host:5432/db`) |
| `NODE_ID`      | nome da máquina                                                              |

**Desvio da spec:** o armazenamento é Postgres, então `DATA_DIR` não é usado; os dados ficam no volume `pgdata` do compose.
Reiniciar com os mesmos dados = rodar a demo de novo apontando para o mesmo `DATABASE_URL`.
Para começar do zero: `docker compose down -v`.

```sh
curl -s -XPOST localhost:8080/demo/jobs -H 'content-type: application/json' \
  -d '{"job":"fail-n-times","params":{"failTimes":2},"options":{"retryDelayMs":500}}'
curl -s localhost:8080/api/jobs/<id>
curl -s localhost:8080/demo/events
```

Painel: abra `http://localhost:8080/jobs` (pacote `BackgroundJobs.Dashboard`, ligado com `app.MapBackgroundJobsDashboard()`; HTML renderizado no servidor, sem JavaScript).

## Testes

```sh
dotnet test
```

Os testes E2E (`tests/BackgroundJobs.E2E/Scenarios`, um arquivo por critério de aceite 1–10, mais `S11_ApiContracts`,
`S12_Observability`, `S13_Dashboard` e `S14_ArgsVersioning`). S01–S07, S11 e S13 rodam duas vezes: no Postgres e no
store em memória (demo com `STORAGE=memory`))
sobem a demo como processo real contra um Postgres do Testcontainers, usando `kill -9` e `SIGTERM` de verdade.
Precisam do Docker rodando.

## Usar a biblioteca sem HTTP

Veja `samples/WorkerExample/Program.cs`:

```csharp
builder.Services
    .AddBackgroundJobs(o => o.FinishedJobRetention = TimeSpan.FromDays(7), registerJobs: jobs => jobs.Register<GreetJob, GreetArgs>("greet"))
    .UsePostgres("...");
// ...
var id = await client.EnqueueAsync("greet", new GreetArgs("now"),
    new JobOptions { RunAt = DateTimeOffset.UtcNow.AddSeconds(2), MaxAttempts = 5 });
```

A API HTTP de gerenciamento é opt-in e fica no pacote `BackgroundJobs.AspNetCore`: `app.MapBackgroundJobsApi()` (`/api/jobs`, `/health` e `/health/details`).

## Comportamento além da spec / limitações

- Vários nós podem compartilhar o banco: claim com `FOR UPDATE SKIP LOCKED`, lease renovado por heartbeat
  (`LeaseDuration`, padrão 30 s). Jobs com lease vencido são recuperados como `interrupted`. Na inicialização,
  jobs presos ao próprio `NODE_ID` são recuperados na hora; por isso dois processos no mesmo host precisam de `NODE_ID`s diferentes.
- Se o heartbeat não conseguir renovar o lease (banco fora por mais que `LeaseDuration`), o job pode ser
  recuperado e executado de novo enquanto a execução original ainda roda (at-least-once); o resultado da
  execução original é descartado pela checagem de `version`/`locked_by`.
- Se o resultado de uma tentativa não puder ser gravado (banco fora), o job só é recuperado quando o lease vence.
- Circuit breaker por tipo de job é opt-in no registro (`o.CircuitBreaker = new(5, TimeSpan.FromSeconds(30))`); a demo só usa em `fail-n-times` quando `FAIL_BREAKER_THRESHOLD` (e opcionalmente `FAIL_BREAKER_SECONDS`) está definido.
- Observabilidade: `QUEUE_MONITOR_MS` e `STALLED_QUEUE_SECONDS` ajustam a amostragem da fila e o limite de fila parada; `GET /demo/telemetry` expõe spans e métricas capturados em memória (substituto de um exporter OpenTelemetry).
- Retenção: `RETENTION_SECONDS` (padrão 7 dias; `forever` mantém tudo) `RETENTION_SWEEP_MS` (padrão 60000) e `RETENTION_BATCH` (padrão 1000) controlam a limpeza de jobs finalizados.
- `GET /api/jobs` aceita `?after=<id>&limit=<1..1000>` (padrão 1000) e devolve `nextCursor`; acima de 1000 jobs é preciso seguir o cursor.
- A parada graciosa não tem limite de tempo; um `SIGTERM` com job longo espera o job terminar.
