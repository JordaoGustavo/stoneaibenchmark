# Guia de Integração — BackgroundJobs

Biblioteca .NET para execução de jobs em segundo plano persistidos em PostgreSQL: agendamento, retry, FIFO, múltiplos workers, recuperação de crash e parada graciosa.

> Para rodar o demo e os testes, veja [`RUNNING.md`](../RUNNING.md). A especificação original está no [`README.md`](../README.md).

## Sumário

1. [Requisitos](#1-requisitos)
2. [Início rápido](#2-início-rápido)
3. [Definindo jobs](#3-definindo-jobs)
4. [Enfileirando e gerenciando jobs (`IJobClient`)](#4-enfileirando-e-gerenciando-jobs-ijobclient)
5. [Configuração](#5-configuração)
6. [Ciclo de vida do job](#6-ciclo-de-vida-do-job)
7. [Retry, FIFO, workers e circuit breaker](#7-retry-fifo-workers-e-circuit-breaker)
8. [API HTTP (opcional)](#8-api-http-opcional)
9. [Dashboard (opcional)](#9-dashboard-opcional)
10. [Health check e observabilidade](#10-health-check-e-observabilidade)
11. [Banco de dados](#11-banco-de-dados)
12. [Operação: múltiplos nós, crash e shutdown](#12-operação-múltiplos-nós-crash-e-shutdown)
13. [Limitações e boas práticas](#13-limitações-e-boas-práticas)

---

## 1. Requisitos

- .NET 10 (`net10.0`; SDK fixado em `global.json`: `10.0.402`, `rollForward: latestFeature`)
- PostgreSQL (testado com `postgres:17-alpine`)
- Dependências do núcleo: só `Microsoft.Extensions.*` (hosting, health checks, métricas). O núcleo **não** depende de ASP.NET Core nem de driver de banco; o pacote Postgres traz `Npgsql` 9.0.3 e `Dapper` 2.1.66.

A biblioteca é dividida em pacotes; referencie só o que usar:

| Pacote | Conteúdo | Depende de |
|---|---|---|
| `BackgroundJobs` | `AddBackgroundJobs`, `IJobClient`, registro de jobs, regras de transição, store em memória, health check | — |
| `BackgroundJobs.Postgres` | `.UsePostgres(connectionString)`: store, migrations e LISTEN/NOTIFY | `BackgroundJobs`, Npgsql, Dapper |
| `BackgroundJobs.AspNetCore` | `MapBackgroundJobsApi()`: `/api/jobs`, `/health`, `/health/details` | `BackgroundJobs`, ASP.NET Core |
| `BackgroundJobs.Dashboard` | `MapBackgroundJobsDashboard()`: UI HTML em `/jobs` | `BackgroundJobs`, ASP.NET Core |

```xml
<ProjectReference Include="path/to/src/BackgroundJobs.Postgres/BackgroundJobs.Postgres.csproj" /> <!-- traz o núcleo -->
<!-- opcionais -->
<ProjectReference Include="path/to/src/BackgroundJobs.AspNetCore/BackgroundJobs.AspNetCore.csproj" />
<ProjectReference Include="path/to/src/BackgroundJobs.Dashboard/BackgroundJobs.Dashboard.csproj" />
```

Os pacotes HTTP usam só a API pública do núcleo (`IJobClient`, `JobRegistry`, `JobServerHealthCheck`).

Todos os tipos públicos ficam no namespace `BackgroundJobs` (o health check está em `BackgroundJobs.Health`).

## 2. Início rápido

```csharp
using BackgroundJobs;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddBackgroundJobs(
        o => o.FinishedJobRetention = TimeSpan.FromDays(7), // obrigatório: veja "Retenção" na seção 5
        jobs => jobs.Register<SendEmailJob, SendEmailArgs>("send-email"))
    .UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs");

var app = builder.Build();
app.MapBackgroundJobsApi();        // opcional (BackgroundJobs.AspNetCore): /health e /api/jobs
app.MapBackgroundJobsDashboard();  // opcional (BackgroundJobs.Dashboard): UI em /jobs
app.Run();

public sealed record SendEmailArgs(string To, string Subject);

public sealed class SendEmailJob(IEmailSender sender) : IJob<SendEmailArgs>
{
    public Task ExecuteAsync(SendEmailArgs args, JobContext context) =>
        sender.SendAsync(args.To, args.Subject);
}
```

Enfileirando:

```csharp
app.MapPost("/signup", async (IJobClient jobs) =>
{
    var id = await jobs.EnqueueAsync("send-email", new SendEmailArgs("a@b.com", "Bem-vindo"));
    return Results.Accepted(value: new { jobId = id });
});
```

Pronto: na inicialização a biblioteca aplica as migrations, recupera jobs interrompidos e começa a processar. Não há passo manual de banco.

Para um processo **sem HTTP** (Worker Service), veja `samples/WorkerExample/Program.cs` — basta `AddBackgroundJobs` em um `Host` genérico.

## 3. Definindo jobs

```csharp
/// Throwing fails the attempt.
public interface IJob<in TArgs> { Task ExecuteAsync(TArgs args, JobContext context); }
public sealed record JobContext(Guid JobId, int Attempt, CancellationToken CancellationToken = default);
```

- **Lançar exceção = tentativa falhou.** A mensagem (ou o nome do tipo, se vazia) é registrada no histórico de tentativas.
- **Argumentos** são serializados com `System.Text.Json` (`JsonSerializerDefaults.Web`: camelCase, case-insensitive) e guardados como `jsonb`. Use records simples e evite mudanças incompatíveis no formato enquanto houver jobs pendentes.
- **Injeção de dependência:** cada execução abre um scope de DI novo e o job é criado com `ActivatorUtilities`, então **não precisa registrar o job no container** — apenas suas dependências. Serviços *scoped* (ex.: `DbContext`) funcionam.
- `context.Attempt` começa em 1; útil para lógica de idempotência ou logs.
- **`context.CancellationToken`** dispara quando a tentativa estoura o timeout ou o servidor está parando. Repasse-o para `HttpClient`, EF, `Task.Delay` etc. O cancelamento é cooperativo:
  - **Timeout** (`DefaultJobTimeout` ou `JobRegistrationOptions.Timeout`): a tentativa falha com erro `timed out after N ms` e segue a política de retry. Se o job ignorar o token, depois de `TimeoutGracePeriod` a tentativa é registrada como falha (`... and did not stop within N ms`) e o worker é liberado; o código do job pode continuar rodando em background (sem limite de quantos), e um retry pode começar enquanto isso. Por isso jobs com timeout precisam ser idempotentes.
  - **Parada do servidor:** se o job terminar com `OperationCanceledException`, a tentativa vira `interrupted` e o job volta para `enqueued` na hora (`interrupted_by_shutdown`), sem esperar o lease — mesmo sem tentativas restantes, para que um deploy nunca falhe um job de vez (o número da tentativa pode passar de `MaxAttempts`). Um job que ignora o token continua sendo esperado.

  ```csharp
  jobs.Register<ReportJob, ReportArgs>("report", o => o.Timeout = TimeSpan.FromMinutes(2));
  ```

### Registro

```csharp
jobs.Register<TJob, TArgs>(string name, Action<JobRegistrationOptions>? configure = null)
```

- `name` é a chave estável persistida no banco. **Não renomeie** um job com instâncias pendentes.
- Nome vazio ou duplicado lança `ArgumentException`.
- Um job enfileirado com nome não registrado falha na execução com erro `"unknown job"` (consome tentativas normalmente).
- O tipo de args faz parte do registro: `EnqueueAsync` com um nome registrado e args de outro tipo lança `ArgumentException`
  (`JsonElement`/`JsonNode` são aceitos como JSON cru no formato atual).

### Versionando os args

Os args são gravados como JSON junto com o job e lidos **quando o job executa** — às vezes depois de um deploy.
Todo job é persistido com a versão de args vigente no registro (coluna `args_version`, padrão `1`; aparece em `Job.ArgsVersion`). A estratégia:

1. **Mudança compatível → não mude a versão.** Adicionar propriedade com valor padrão, remover propriedade
   ou tornar algo opcional: o desserializador (`System.Text.Json`, camelCase) ignora campos desconhecidos e usa o
   padrão para campos ausentes. Para isso funcionar, propriedades novas precisam ter padrão
   (`record GreetArgs(string Who, string Greeting = "olá")`).
2. **Mudança incompatível → incremente `ArgsVersion` e registre um upgrade** da versão anterior. Renomear,
   mudar tipo ou dividir um campo são incompatíveis:

   ```csharp
   // v1: record GreetArgs(string Who)   v2: record GreetArgs(string FirstName, string LastName)
   jobs.Register<GreetJob, GreetArgs>("greet", o =>
   {
       o.ArgsVersion = 2;
       o.UpgradeArgs(1, v1 =>
       {
           var parts = v1["who"]!.GetValue<string>().Split(' ', 2);
           return new JsonObject { ["firstName"] = parts[0], ["lastName"] = parts.ElementAtOrDefault(1) ?? "" };
       });
   });
   ```

   - Ao executar um job gravado na versão N < atual, os upgrades N→N+1→… são aplicados em memória antes de
     desserializar. **Os args gravados não são reescritos** (o histórico continua mostrando o que foi enfileirado).
   - `Register` exige um upgrade para cada versão de `1` até `ArgsVersion - 1`; faltando algum, lança `ArgumentException`
     na inicialização — nunca em produção na hora de executar.
   - Upgrades recebem e devolvem `JsonObject` (nomes em camelCase); mantenha-os para sempre, ou até ter certeza
     de que não há mais jobs pendentes na versão antiga.
3. **Job com versão mais nova que o código** (rollback, ou nó antigo durante um rolling deploy): a tentativa falha
   com `args version 2 of job 'greet' is newer than the registered version 1` em vez de desserializar errado.
   A falha consome tentativas como qualquer outra; depois do deploy, use **requeue** nos que esgotaram.
   Para evitar: faça deploy primeiro do código que **lê** a versão nova e só depois do que **enfileira** com ela,
   ou use outro nome de job (`greet-v2`) para mudanças grandes.

Os cenários `tests/BackgroundJobs.E2E/Scenarios/S14_ArgsVersioning.cs` exercitam exatamente esse fluxo com dois deploys do demo.

## 4. Enfileirando e gerenciando jobs (`IJobClient`)

`IJobClient` é singleton e pode ser injetado em qualquer lugar.

| Método | Descrição | Exceções |
|---|---|---|
| `EnqueueAsync<TArgs>(name, args, JobOptions? options = null, ct)` | Persiste e retorna o `Guid` (UUIDv7) | `ArgumentException` (nome vazio, opções inválidas) |
| `GetAsync(id, ct)` | Retorna o `Job` completo, com histórico e tentativas | `JobNotFoundException` |
| `ListAsync(Guid? after = null, int limit = 100, ct)` | Resumos em ordem de criação; paginação por cursor (`after` = último id visto). `limit` 1..1000 | `ArgumentException` |
| `DeleteAsync(id, ct)` | Remove da fila (só `scheduled`/`enqueued`) | `JobNotFoundException`, `InvalidJobTransitionException` |
| `RequeueAsync(id, ct)` | Reexecuta (só `succeeded`/`failed`/`deleted`), zera tentativas, mantém histórico | `JobNotFoundException`, `InvalidJobTransitionException` |

Erros de banco em chamadas do `IJobClient` **propagam para quem chamou** (não há retry automático no cliente).

### `JobOptions` (por job)

Campos não informados usam os padrões de `BackgroundJobsOptions`.

| Campo | Significado |
|---|---|
| `RunAt` (`DateTimeOffset?`) | Não executar antes desse instante. Nulo ou no passado = agora |
| `MaxAttempts` (`int?`) | Total de execuções, incluindo a primeira (≥ 1) |
| `RetryDelay` (`TimeSpan?`) | Espera fixa entre tentativas (≥ 0) |
| `RetryJitter` (`TimeSpan?`) | Atraso aleatório extra em `[0, RetryJitter]` somado a cada retry |

Agendamento não tem método próprio — é `EnqueueAsync` com `RunAt`:

```csharp
await client.EnqueueAsync("greet", new GreetArgs("em 2s"), new JobOptions
{
    RunAt = DateTimeOffset.UtcNow.AddSeconds(2),
    MaxAttempts = 5,
    RetryDelay = TimeSpan.FromMilliseconds(500),
});
```

### Modelos retornados

```csharp
record JobSummary(Guid Id, string Name, JobState State, int Attempts);
record Job(Guid Id, string Name, JsonElement Args, int ArgsVersion, JobState State, int Attempts, int MaxAttempts,
           TimeSpan RetryDelay, DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt,
           IReadOnlyList<JobHistoryEntry> History, IReadOnlyList<JobAttempt> AttemptHistory);
record JobHistoryEntry(JobState State, DateTime At, string Reason);
record JobAttempt(int Attempt, DateTime StartedAt, DateTime? FinishedAt, AttemptOutcome? Outcome, string? Error);
```

Datas em UTC (precisão de microssegundos, igual ao Postgres).

## 5. Configuração

`AddBackgroundJobs(Action<BackgroundJobsOptions>? configure = null, Action<JobRegistry>? registerJobs = null)` devolve um
`BackgroundJobsBuilder`, no qual se escolhe **um** store: `.UsePostgres(connectionString)` (pacote `BackgroundJobs.Postgres`,
connection string Npgsql `Host=...;Database=...`) ou `.UseInMemoryStorage()`. Sem store, a aplicação falha ao iniciar com
`InvalidOperationException: no job store configured`.

| Opção | Padrão | Descrição |
|---|---|---|
| `FinishedJobRetention` | **obrigatório** | Quanto tempo um job finalizado (`succeeded`, `failed`, `deleted`) é mantido, com histórico e tentativas. `Timeout.InfiniteTimeSpan` mantém para sempre |
| `RetentionSweepInterval` | `1min` | Frequência da limpeza de jobs finalizados |
| `RetentionBatchSize` | `1000` | Jobs removidos por comando de limpeza (a limpeza repete até esgotar) |
| `WorkerCount` | `4` | Máximo de jobs simultâneos neste processo (≥ 1) |
| `NodeId` | `Environment.MachineName` | Identidade do processo como dono do lease. **Único por processo** |
| `LeaseDuration` | `30s` | Tempo até outro nó considerar o job abandonado (> 0) |
| `PollInterval` | `250ms` | Polling de fallback da fila (o caminho normal é LISTEN/NOTIFY do Postgres) |
| `SchedulerMaxWait` | `5s` | Espera máxima do agendador; normalmente dorme até o próximo `run_at` ou acorda por notificação |
| `ClaimBatchSize` | `null` | Máximo de jobs por claim; `null` = tantos quanto houver slots livres (nunca acima de `WorkerCount`) |
| `DefaultMaxAttempts` | `3` | Padrão de `JobOptions.MaxAttempts` |
| `DefaultRetryDelay` | `1s` | Padrão de `JobOptions.RetryDelay` |
| `DefaultRetryJitter` | `0` | Padrão de `JobOptions.RetryJitter` |
| `DefaultJobTimeout` | `Timeout.InfiniteTimeSpan` | Tempo máximo de uma tentativa; `JobRegistrationOptions.Timeout` sobrescreve por tipo |
| `TimeoutGracePeriod` | `5s` | Após o timeout, quanto esperar um job que ignora o token antes de abandoná-lo |

Configuração inválida lança `ArgumentException` ao iniciar o host.

### Retenção

Jobs finalizados, seu histórico e suas tentativas crescem sem limite se ninguém os apagar, por isso
`FinishedJobRetention` **não tem padrão**: sem ela a aplicação não inicia. Cada nó varre a cada `RetentionSweepInterval`
e remove de vez os jobs em `succeeded`, `failed` ou `deleted` cuja última transição é mais antiga que a retenção (depois
disso o `GET` devolve 404 e não dá mais para fazer requeue). Jobs `scheduled`, `enqueued` e `processing` nunca são
removidos. Um requeue reinicia a contagem. Para manter tudo (e cuidar da limpeza por conta própria), use
`Timeout.InfiniteTimeSpan`.

A biblioteca **não lê** `IConfiguration` sozinha; faça o bind você mesmo:

```csharp
builder.Services.AddBackgroundJobs(o =>
{
    o.FinishedJobRetention = builder.Configuration.GetValue<TimeSpan?>("Jobs:Retention");
    o.WorkerCount = builder.Configuration.GetValue("Jobs:WorkerCount", 4);
    o.NodeId = Environment.GetEnvironmentVariable("NODE_ID") ?? Environment.MachineName;
}, jobs => ...)
.UsePostgres(builder.Configuration.GetConnectionString("Jobs")!);
```

> URLs no formato `postgres://user:pass@host:5432/db` **não** são aceitas pela biblioteca; o projeto Demo tem um conversor (`ToConnectionString` em `src/BackgroundJobs.Demo/Program.cs`) que pode ser copiado.

## 6. Ciclo de vida do job

Estados (`JobState`, no JSON em minúsculas): `scheduled`, `enqueued`, `processing`, `succeeded`, `failed`, `deleted`.

```
             RunAt futuro                    became_due
 criação ──────────────────► scheduled ───────────────────► enqueued ──claimed──► processing
    │                           ▲   │                          ▲                     │
    └──────── created ──────────┼───┼──────────────────────────┘                     │
                                │   └── deleted_by_user ──► deleted                  │
                 retry_scheduled│                                                    │
                                └────────────── falhou e restam tentativas ◄─────────┤
                                                                                     ├─► succeeded
                                                         tentativas esgotadas ───────┴─► failed

 succeeded / failed / deleted ── requeued_by_user ──► enqueued   (tentativas zeradas, histórico mantido)
 processing ── crash/lease expirado ──► enqueued (recovered_after_crash) ou failed se sem tentativas
 processing ── cancelado pela parada ──► enqueued (interrupted_by_shutdown), sempre
```

| Operação | Estados de origem permitidos |
|---|---|
| Delete | `scheduled`, `enqueued` |
| Requeue | `succeeded`, `failed`, `deleted` |

Um job em `processing` **não pode ser deletado**. As transições usam concorrência otimista (`version`); operações concorrentes conflitantes resultam em `InvalidJobTransitionException`.

Resultados de tentativa (`AttemptOutcome`): `succeeded`, `failed`, `interrupted` (processo morreu ou parou durante a execução — conta como tentativa).

Os motivos do histórico estão em constantes públicas na classe `Reasons`.

## 7. Retry, FIFO, workers e circuit breaker

- **Retry:** atraso **fixo** (`RetryDelay` + jitter aleatório opcional), não exponencial. A tentativa falha volta a `scheduled` e é promovida quando vencer.
- **FIFO:** uma única fila global (sem filas nomeadas). A ordem é por entrada em `enqueued`; retries, recuperações e requeues vão para o fim da fila. Com `WorkerCount = 1` a ordem de execução é estrita.
- **Workers:** um job só é reivindicado quando há slot livre (sem prefetch), então nunca roda mais que `WorkerCount` por processo. Vários processos podem compartilhar o banco (`FOR UPDATE SKIP LOCKED`).
- **Circuit breaker por tipo de job (opt-in):** após N falhas consecutivas, o tipo para de ser reivindicado por um tempo; depois deixa passar um único job de teste (half-open).

  ```csharp
  jobs.Register<ChargeJob, ChargeArgs>("charge",
      o => o.CircuitBreaker = new CircuitBreakerOptions(FailureThreshold: 5, BreakDuration: TimeSpan.FromSeconds(30)));
  ```

  O estado do breaker é em memória, por processo. Jobs bloqueados ficam em `enqueued`.
- **Breaker do banco (automático):** se o Postgres cair, os loops internos fazem backoff exponencial (100 ms → 5 s) até voltar. Não é configurável.

### Store em memória (testes)

```csharp
builder.Services.AddBackgroundJobs(o => o.FinishedJobRetention = TimeSpan.FromHours(1), jobs => ...).UseInMemoryStorage();
```

Troca o Postgres por um store no próprio processo, com as mesmas transições, histórico, FIFO, retry, agendamento,
delete/requeue e versão de args — útil para testes rápidos de quem integra a biblioteca, sem Docker.
Não persiste nada (tudo some quando o processo termina) e não é compartilhado entre processos: **não use em produção**.
O demo roda com ele via `STORAGE=memory`.

### Escrevendo um novo store

Um store (SQL Server, Redis, ...) é um pacote próprio que referencia só o núcleo e implementa o SPI público em
`BackgroundJobs.Storage`:

- **`IJobStore`** — persiste jobs. **Não decide regras**: quais estados podem sair para onde, com que motivo de histórico
  e se zera tentativas vem pronto em `JobTransition` (`Promote`, `Claim`, `Succeed`, `Fail`, `Recover`, `Delete`, `Requeue`),
  junto com `NewJob.Creation`. O store só aplica a transição de forma atômica, seguindo a mecânica comum descrita no
  comentário de `IJobStore`: guarda por estado de origem + versão (+ dono), incrementa a versão, limpa o lock, sequência
  FIFO ao entrar em `enqueued`, grava o histórico.
- **`IJobStoreListener`** (opcional) — acorda os loops quando *outro nó* muda o store (no Postgres, LISTEN/NOTIFY), via
  `IJobSignals`. As mudanças do próprio nó já acordam os loops pelo núcleo; sem listener, outros nós são vistos no polling.
- Um método de extensão `UseXxx(this BackgroundJobsBuilder)` que faz `Services.Replace` de `IJobStore` e `IJobStoreListener`
  (veja `src/BackgroundJobs.Postgres/PostgresBuilderExtensions.cs`).

**Conformidade:** os cenários E2E que não dependem de durabilidade (S01–S07, S11, S13) rodam contra **todos** os stores
de `Stores.All` (`tests/BackgroundJobs.E2E/Infra/PostgresFixture.cs`). Para validar um store novo, adicione-o ali e ao
demo (`STORAGE=...`): o mesmo comportamento observável é o contrato. Cenários de restart/crash/parada (S08–S10) exigem
store durável.

## 8. API HTTP (opcional)

Pacote `BackgroundJobs.AspNetCore`. `app.MapBackgroundJobsApi()` — expõe:

| Método e rota | Sucesso | Erros |
|---|---|---|
| `GET /health` | `200 {"status":"ok"}` | `503 {"status":"unavailable"}` |
| `GET /health/details` | `200` relatório (healthy/degraded) — ver [§10](#10-health-check-e-observabilidade) | `503` quando não está processando |
| `GET /api/jobs?after=<id>&limit=<1..1000>` | `200 {"items":[{"id","name","state","attempts"}],"nextCursor":"<id>"\|null}` | `400` |
| `GET /api/jobs/{id}` | `200` job completo | `404` |
| `DELETE /api/jobs/{id}` | `200 {"id":"...","state":"deleted"}` | `404`, `409` |
| `POST /api/jobs/{id}/requeue` | `202 {"jobId":"..."}` | `404`, `409` |

- Erros sempre no formato `{"error":"mensagem"}`. Id que não é Guid → `404`.
- `limit` padrão 1000; `nextCursor` só vem preenchido quando a página está cheia.
- Job completo (ordem das chaves): `id, name, args, state, attempts, maxAttempts, retryDelayMs, scheduledAt, createdAt, updatedAt, history[{state,at,reason}], attemptHistory[{attempt,startedAt,finishedAt,outcome,error}]`. Datas ISO-8601 UTC com `Z`; `outcome` é `null` enquanto a tentativa roda.

A API **não cria jobs** — enfileiramento é responsabilidade da sua aplicação via `IJobClient` (o Demo mostra um exemplo em `POST /demo/jobs`). Para reutilizar o formato de erro: `ManagementEndpoints.Error(status, message)`.

**Proteja as rotas** se expostas publicamente:

```csharp
app.MapBackgroundJobsApi();  // retorna IEndpointRouteBuilder
```

Coloque-as atrás de autenticação/rede interna conforme sua infraestrutura.

## 9. Dashboard (opcional)

Pacote `BackgroundJobs.Dashboard`. `app.MapBackgroundJobsDashboard(string prefix = "/jobs")` — UI HTML renderizada no servidor (sem JavaScript), com modo escuro.

- `GET {prefix}/` — lista paginada (50 por página), filtro por estado, auto-refresh a cada 5 s (desligável com `?refresh=off`).
- `GET {prefix}/{id}` — detalhes, histórico, tentativas com duração e erro, args em JSON; botões Requeue/Delete quando a transição é válida.
- `POST {prefix}/{id}/delete` e `POST {prefix}/{id}/requeue` — ações via formulário.

Segurança embutida: CSP restritiva, `nosniff`, `no-referrer`, `no-store` e rejeição (`403`) de POSTs cross-site via `Sec-Fetch-Site`. **Não há autenticação embutida** — recomendado:

```csharp
app.MapBackgroundJobsDashboard().RequireAuthorization("admin");
```

> O filtro por estado varre a lista em ordem de id (até 10.000 jobs por página) por não haver índice por estado; em bases muito grandes pode ser lento.

## 10. Health check e observabilidade

### Health check

`JobServerHealthCheck` (registrado automaticamente):

- **Unhealthy** fora do intervalo "migrou/recuperou" → "pedido de parada".
- **Degraded** quando a fila está parada (algum job em `enqueued` há mais que `StalledQueueThreshold`, padrão 60 s) ou algum circuit breaker de job está `open` (`half-open` não degrada: já deixa passar um job de teste).
- **Healthy** caso contrário. O `data` do resultado traz `ready`, `queue.*` e o estado de cada breaker.

```csharp
builder.Services.AddHealthChecks().AddCheck<JobServerHealthCheck>("jobs");
app.MapHealthChecks("/ready");
```

Ou use as rotas de `MapBackgroundJobsApi()`: `/health` (readiness, contrato da spec) e `/health/details`:

```json
{
  "status": "degraded", "ready": true,
  "queue": { "ready": 12, "oldestReadyAgeSeconds": 74.2, "stalled": true, "stalledThresholdSeconds": 60, "sampledAt": "…Z" },
  "circuitBreakers": [ { "job": "send-email", "state": "open", "consecutiveFailures": 5, "openUntil": "…Z" } ]
}
```

A fila é amostrada a cada `QueueMonitorInterval` (padrão 5 s), então idade e profundidade podem estar até um intervalo atrasadas; `queue.ready` é `null` antes da primeira amostra. Jobs bloqueados por breaker aberto continuam em `enqueued` e contam para a fila parada.

### OpenTelemetry

A biblioteca usa `ActivitySource` e `Meter` nativos com o nome `BackgroundJobsTelemetry.Name` (`"BackgroundJobs"`):

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(BackgroundJobsTelemetry.Name))
    .WithMetrics(m => m.AddMeter(BackgroundJobsTelemetry.Name));
```

- **Span por tentativa** `job <name>` (kind `Consumer`, raiz — o enqueue não propaga contexto). Tags: `job.id`, `job.name`, `job.attempt`, `job.max_attempts`, `job.run`, `job.outcome`; status `Error` com a exceção como evento quando falha.
- **Métricas:**

| Instrumento | Tipo | Unidade | Tags |
|---|---|---|---|
| `backgroundjobs.attempt.duration` | histogram | `s` | `job.name`, `outcome` |
| `backgroundjobs.attempts` | counter | `{attempt}` | `job.name`, `outcome` (`succeeded`/`failed`) — taxa de falha = `failed / total` |
| `backgroundjobs.queue.depth` | gauge | `{job}` | — jobs em `enqueued` |
| `backgroundjobs.queue.oldest_age` | gauge | `s` | — idade do job mais antigo em `enqueued` (0 se vazia) |

Os gauges vêm da mesma amostra do health check e são globais ao banco (todos os nós reportam o mesmo valor).

### Logs estruturados

Todo log emitido durante uma tentativa — inclusive os do próprio job via `ILogger` — tem no scope `JobId`, `JobName`, `Attempt` e `Run`. Habilite scopes no seu provider (ex.: `Logging:Console:FormatterOptions:IncludeScopes=true`).

## 11. Banco de dados

- Migrations embutidas são aplicadas **automaticamente** na partida, numa transação protegida por advisory lock (seguro com vários nós subindo juntos). Controle em `bj_migrations`.
- Tabelas: `bj_jobs`, `bj_attempts`, `bj_history` (+ sequence `bj_enqueue_seq`). O usuário do banco precisa de permissão de `CREATE` no schema na primeira execução.
- **Não há limpeza automática** de jobs antigos. Se o volume for alto, crie uma rotina própria (ex.: apagar `succeeded` antigos — `bj_attempts`/`bj_history` referenciam `bj_jobs` por FK).

## 12. Operação: múltiplos nós, crash e shutdown

- **Múltiplos nós:** suportado. Cada processo deve ter `NodeId` **único** — na partida, um nó recupera imediatamente todos os jobs presos com o seu próprio `NodeId`. Dois processos com o mesmo id roubariam jobs um do outro.
- **Lease e heartbeat:** cada job em execução tem um lease de `LeaseDuration`, renovado a cada `LeaseDuration / 3`. Se o nó morrer, outro nó recupera o job após o lease expirar; a tentativa vira `interrupted` e conta para `MaxAttempts`.
- **Reinício com mesmo `NodeId`:** recupera os jobs desse nó na hora, sem esperar o lease.
- **Parada graciosa:** ao parar, o servidor deixa de pegar novos jobs sinaliza `context.CancellationToken` e **espera os em execução terminarem, sem limite de tempo**; jobs que param pelo token voltam para a fila na hora (o HTTP continua aceitando enqueues durante o drain). Ajuste o timeout do host conforme sua plataforma:

  ```csharp
  builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = Timeout.InfiniteTimeSpan);
  ```

  Em Kubernetes/ECS, alinhe `terminationGracePeriodSeconds` com a duração dos seus jobs mais longos.
- **Partida:** `StartAsync` retorna imediatamente; migração e recuperação são refeitas em background até o banco responder.

## 13. Limitações e boas práticas

- **Entrega at-least-once:** um job pode executar mais de uma vez (crash, ou banco indisponível por mais que `LeaseDuration` durante a execução). **Escreva jobs idempotentes.**
- **Cancelamento cooperativo:** jobs que ignoram o token atrasam o shutdown e, ao estourar o timeout, são abandonados mas não interrompidos.
- **Retry fixo**, sem backoff exponencial nativo (use `RetryJitter` para espalhar).
- **Fila única**, sem prioridades nem filas nomeadas.
- **Circuit breaker de job** é por processo, não compartilhado entre nós.
- **Sem retenção** automática de jobs concluídos.
- Mantenha os args pequenos (são `jsonb` e voltam em toda consulta de detalhe).

## Referências no código

| Tema | Arquivo |
|---|---|
| Registro / DI | `src/BackgroundJobs/DependencyInjection/ServiceCollectionExtensions.cs` |
| Contratos públicos | `src/BackgroundJobs/Abstractions/` |
| Estados e transições | `src/BackgroundJobs/Domain/` |
| Versionamento de args | `src/BackgroundJobs/Registry/JobRegistry.cs` |
| Regras de transição | `src/BackgroundJobs/Domain/Transitions.cs` |
| SPI de store | `src/BackgroundJobs/Storage/IJobStore.cs` |
| Store em memória | `src/BackgroundJobs/Storage/InMemory/InMemoryJobStore.cs` |
| Store Postgres | `src/BackgroundJobs.Postgres/` |
| API HTTP | `src/BackgroundJobs.AspNetCore/ManagementEndpoints.cs` |
| Dashboard | `src/BackgroundJobs.Dashboard/DashboardEndpoints.cs` |
| Schema | `src/BackgroundJobs.Postgres/Migrations/` |
| Exemplo sem HTTP | `samples/WorkerExample/Program.cs` |
| Exemplo com HTTP | `src/BackgroundJobs.Demo/Program.cs` |
