# Referência da API pública

Referência compacta de todos os tipos públicos dos quatro pacotes, derivada do código-fonte. Explicações e exemplos
estão no [Guia de Integração](INTEGRACAO.md). Salvo indicação, tudo fica no namespace `BackgroundJobs`. Datas
(`DateTime`) são UTC.

Sumário: [Registro e DI](#registro-e-di) · [Jobs](#definindo-jobs) · [IJobClient](#ijobclient) ·
[Opções](#opções) · [Modelos](#modelos) · [Recorrentes](#jobs-recorrentes) · [Exceções](#exceções) ·
[Estados e transições](#estados-e-transições) · [Health](#health-check-backgroundjobshealth) ·
[Telemetria](#telemetria) · [SPI de store](#spi-de-store-backgroundjobsstorage) · [Postgres](#backgroundjobspostgres) ·
[API HTTP](#backgroundjobsaspnetcore-api-http) · [Dashboard](#backgroundjobsdashboard)

---

## Registro e DI

Pacote `BackgroundJobs`.

```csharp
static BackgroundJobsBuilder AddBackgroundJobs(this IServiceCollection services,
    Action<BackgroundJobsOptions>? configure = null, Action<JobRegistry>? registerJobs = null)
```
Registra `IJobClient`, `IRecurringJobClient`, `JobRegistry`, `JobServerHealthCheck` e o servidor hospedado (todos
singleton). As opções são validadas na partida do host (`ValidateOnStart`): opção inválida lança `ArgumentException`
(dentro de `OptionsValidationException`). Exige um store: sem `.UsePostgres` nem `.UseInMemoryStorage()`, resolver o store
lança `InvalidOperationException` ("no job store configured...").

### `sealed class BackgroundJobsBuilder`

| Membro | Contrato |
|---|---|
| `IServiceCollection Services { get; }` | A coleção de serviços, para extensões de store (`Services.Replace`) |
| `BackgroundJobsBuilder UseInMemoryStorage()` | Store no próprio processo. Perde tudo ao sair e não é compartilhado entre nós. Não use em produção |
| `BackgroundJobsBuilder AddRecurringJob<TArgs>(string id, string jobName, string cron, TArgs args, RecurringJobOptions? options = null)` | Faz upsert da definição a cada partida do servidor. `ArgumentException` imediato para id ou cron inválidos; job não registrado (ou opções inválidas) só aparece no upsert, na partida: o erro é **logado** e o servidor sobe sem a definição. Remover a chamada não apaga a definição |

### `sealed class JobRegistry`

| Membro | Contrato |
|---|---|
| `JobRegistry Register<TJob, TArgs>(string name, Action<JobRegistrationOptions>? configure = null) where TJob : IJob<TArgs>` | `ArgumentException` para nome vazio ou já registrado, `ArgsVersion < 1`, upgrade faltando para alguma versão `1..ArgsVersion-1`, upgrade com `fromVersion >= ArgsVersion`, `MaxConcurrency < 1`, `Timeout` que não é positivo nem infinito. Pode ser chamado depois da partida |
| `IReadOnlyList<(string Name, Type ArgsType)> Registrations { get; }` | Jobs registrados, ordenados por nome |

O job é criado por `ActivatorUtilities` num scope de DI novo a cada tentativa, então não precisa estar registrado no
container.

### `sealed class JobRegistrationOptions`

| Membro | Padrão | Contrato |
|---|---|---|
| `CircuitBreakerOptions? CircuitBreaker { get; set; }` | `null` | Breaker por tipo, por processo |
| `int ArgsVersion { get; set; }` | `1` | Versão do formato dos args que este código lê e grava |
| `int? MaxConcurrency { get; set; }` | `null` (sem limite) | Máximo de jobs deste nome em `processing` no cluster inteiro. Configure igual em todos os nós |
| `TimeSpan? Timeout { get; set; }` | `null` (usa `DefaultJobTimeout`) | Positivo ou `Timeout.InfiniteTimeSpan` |
| `JobRegistrationOptions UpgradeArgs(int fromVersion, Func<JsonObject, JsonObject> upgrade)` | | Converte args da versão `fromVersion` para `fromVersion + 1`, em memória, ao executar. `ArgumentOutOfRangeException` se `fromVersion < 1`; `ArgumentNullException`; `ArgumentException` se já houver upgrade para essa versão |

```csharp
sealed record CircuitBreakerOptions(int FailureThreshold, TimeSpan BreakDuration);
```
Depois de `FailureThreshold` falhas consecutivas o tipo deixa de ser pego por `BreakDuration`; então um único job de
teste passa (half-open).

```csharp
static class JobJson { static readonly JsonSerializerOptions Options; } // JsonSerializerDefaults.Web
```

## Definindo jobs

```csharp
interface IJob<in TArgs> { Task ExecuteAsync(TArgs args, JobContext context); }
sealed record JobContext(Guid JobId, int Attempt, CancellationToken CancellationToken = default);
```
- Lançar exceção faz a tentativa falhar. A mensagem é gravada como erro da tentativa.
- `Attempt` começa em 1 e nunca se repete dentro da execução (tentativas devolvidas mantêm seu número).
- `CancellationToken` dispara em timeout, cancelamento pelo usuário, parada do servidor ou perda do lease. O cancelamento
  é cooperativo.

## `IJobClient`

Singleton. Erros do banco propagam para quem chamou.

| Membro | Contrato |
|---|---|
| `Task<Guid> EnqueueAsync<TArgs>(string jobName, TArgs args, JobOptions? options = null, CancellationToken ct = default)` | Persiste e retorna o id (UUIDv7). `RunAt` futuro → `scheduled`; senão `enqueued`. Nome desconhecido é **aceito**; só nós que registraram o nome fazem claim dele, então o job fica em `enqueued` (sem gastar tentativas, fila aparece parada no health) até subir um nó que o registre. Com `IdempotencyKey` em uso por job não finalizado, não cria nada e retorna o id dele. `ArgumentException`: nome vazio, chave em branco ou com mais de 200 caracteres, `MaxAttempts < 1`, `RetryDelay`/`RetryJitter` negativos ou acima de `int.MaxValue` ms, fila inválida, prioridade fora de `-100..100`, args de tipo diferente do registrado (`JsonElement`/`JsonNode` são aceitos) |
| `Task<IReadOnlyList<Guid>> EnqueueManyAsync(IEnumerable<JobToEnqueue> jobs, CancellationToken ct = default)` | Enfileiramento em lote, **atômico** (todos ou nenhum), numa chamada ao store (no Postgres, um único `INSERT ... SELECT FROM unnest(...)`). Retorna os ids na ordem de entrada. Cada job é validado como no `EnqueueAsync` antes de gravar qualquer coisa; o primeiro inválido lança `ArgumentException` com `jobs[i]` na mensagem e nada é criado. `IdempotencyKey` por job: chave em uso por job não finalizado, **ou por um job anterior do mesmo lote**, não cria nada e devolve o id do dono. Os jobs que entram em `enqueued` ficam na ordem do lote (FIFO). Lote vazio não grava nada |
| `Task<Job> GetAsync(Guid id, CancellationToken ct = default)` | `JobNotFoundException` |
| `Task<IReadOnlyList<JobSummary>> ListAsync(Guid? after = null, int limit = 100, bool descending = false, CancellationToken ct = default)` | Ordem de id (= criação), crescente a menos que `descending`. `after` = último id visto. `ArgumentException` se `limit` estiver fora de `1..1000` |
| `Task DeleteAsync(Guid id, CancellationToken ct = default)` | Só `scheduled`/`enqueued` → `deleted`. `JobNotFoundException`, `InvalidJobTransitionException` |
| `Task RequeueAsync(Guid id, CancellationToken ct = default)` | Só estados finalizados → `enqueued`, tentativas zeradas, histórico mantido. `JobNotFoundException`, `InvalidJobTransitionException`, `IdempotencyKeyConflictException` |
| `Task CancelAsync(Guid id, CancellationToken ct = default)` | `scheduled`/`enqueued` → `cancelled` na hora; `processing`: dispara o token (na hora neste nó, em até `LeaseDuration / 3` em outro) e termina em `cancelled`, a menos que o job conclua com sucesso. `JobNotFoundException`, `InvalidJobTransitionException` |

### `sealed record JobToEnqueue(string JobName, object? Args, JobOptions? Options = null)`

Um job do lote de `EnqueueManyAsync`. `Args` é serializado pelo tipo em tempo de execução e, para um nome registrado,
precisa ser do tipo registrado (ou `JsonElement`/`JsonNode`).

## Opções

### `sealed record JobOptions` (por job)

Campos `null` usam os padrões de `BackgroundJobsOptions`.

| Membro | Contrato |
|---|---|
| `DateTimeOffset? RunAt { get; init; }` | Não antes deste instante. Nulo ou no passado = agora |
| `int? MaxAttempts { get; init; }` | Execuções no total, incluindo a primeira; ≥ 1 |
| `TimeSpan? RetryDelay { get; init; }` | Atraso fixo entre tentativas; ≥ 0 |
| `TimeSpan? RetryJitter { get; init; }` | Atraso extra aleatório em `[0, RetryJitter]` por retry; ≥ 0 |
| `string? Queue { get; init; }` | Padrão `JobQueues.Default`. Veja `JobQueues.IsValidName` |
| `int? Priority { get; init; }` | `-100..100`, padrão `0`; maior roda primeiro na fila |
| `string? IdempotencyKey { get; init; }` | Deduplica enquanto o job com a chave não finalizou. Não pode ser em branco |
| `const int MaxIdempotencyKeyLength = 200` | |

### `sealed class BackgroundJobsOptions`

| Propriedade | Padrão | Validação / contrato |
|---|---|---|
| `TimeSpan? FinishedJobRetention` | **nenhum (obrigatório)** | Positivo ou `Timeout.InfiniteTimeSpan`. Jobs finalizados mais antigos são removidos com histórico e tentativas |
| `TimeSpan RetentionSweepInterval` | `1 min` | > 0 |
| `int RetentionBatchSize` | `1000` | ≥ 1 |
| `int WorkerCount` | `4` | ≥ 1; só vale quando `Queues` está vazio |
| `Dictionary<string, int> Queues` | vazio | Fila → workers (≥ 1); nomes válidos. Vazio = só `default` com `WorkerCount` |
| `string NodeId` | `MachineName-<8 hex aleatórios>` | Precisa ser único entre os processos vivos |
| `TimeSpan LeaseDuration` | `30 s` | > 0; heartbeat a cada `LeaseDuration / 3` |
| `TimeSpan PollInterval` | `250 ms` | Polling de fallback |
| `TimeSpan SchedulerMaxWait` | `5 s` | > 0 |
| `int? FifoClaimEvery` | `10` | ≥ 2, ou `null` para prioridade estrita |
| `int? ClaimBatchSize` | `null` | ≥ 1; `null` = slots livres da fila |
| `int DefaultMaxAttempts` | `3` | |
| `TimeSpan DefaultRetryDelay` | `1 s` | |
| `TimeSpan DefaultRetryJitter` | `0` | |
| `TimeSpan DefaultJobTimeout` | `Timeout.InfiniteTimeSpan` | Positivo ou infinito |
| `TimeSpan TimeoutGracePeriod` | `5 s` | ≥ 0 |
| `int MaxAbandonedJobs` | `4` | ≥ 0 |
| `TimeSpan AbandonedJobAlertAfter` | `5 min` | ≥ 0 |
| `TimeSpan ShutdownTimeout` | `Timeout.InfiniteTimeSpan` | ≥ 0 ou infinito |
| `TimeSpan QueueMonitorInterval` | `5 s` | > 0 |
| `TimeSpan StalledQueueThreshold` | `60 s` | > 0 |

A biblioteca não lê `IConfiguration`: faça o bind no delegate `configure`.

### `static class JobQueues`

| Membro | Contrato |
|---|---|
| `const string Default = "default"` | |
| `const int MinPriority = -100, MaxPriority = 100, DefaultPriority = 0` | |
| `static bool IsValidName(string? name)` | `^[a-z0-9][a-z0-9_.-]{0,63}$` |

## Modelos

```csharp
sealed record Job(Guid Id, string Name, JsonElement Args, int ArgsVersion, JobState State, int Attempts, int MaxAttempts,
    TimeSpan RetryDelay, DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt,
    IReadOnlyList<JobHistoryEntry> History, IReadOnlyList<JobAttempt> AttemptHistory,
    int RefundedAttempts = 0, string Queue = JobQueues.Default, int Priority = JobQueues.DefaultPriority,
    string? IdempotencyKey = null);
sealed record JobSummary(Guid Id, string Name, JobState State, int Attempts, DateTime? ScheduledAt, DateTime CreatedAt,
    DateTime UpdatedAt, string Queue = JobQueues.Default, int Priority = JobQueues.DefaultPriority);
sealed record JobHistoryEntry(JobState State, DateTime At, string Reason);
sealed record JobAttempt(int Attempt, DateTime StartedAt, DateTime? FinishedAt, AttemptOutcome? Outcome, string? Error);
```
`Attempts - RefundedAttempts` é o que conta contra `MaxAttempts`. `Outcome` é `null` enquanto a tentativa roda.

## Jobs recorrentes

### `IRecurringJobClient`

| Membro | Contrato |
|---|---|
| `Task<RecurringJob> UpsertAsync<TArgs>(string id, string jobName, string cron, TArgs args, RecurringJobOptions? options = null, CancellationToken ct = default)` | Cria ou substitui. Mantém a ocorrência pendente se cron e fuso não mudaram. `ArgumentException`: id ou cron inválidos, job **não registrado**, opções inválidas, cron que nunca dispara |
| `Task<RecurringJob> GetAsync(string id, CancellationToken ct = default)` | `RecurringJobNotFoundException` |
| `Task<IReadOnlyList<RecurringJob>> ListAsync(CancellationToken ct = default)` | Ordenado por id |
| `Task RemoveAsync(string id, CancellationToken ct = default)` | Para as próximas ocorrências; jobs já criados ficam. `RecurringJobNotFoundException` |
| `Task<Guid> TriggerAsync(string id, CancellationToken ct = default)` | Enfileira um job agora sem mover a próxima ocorrência. `RecurringJobNotFoundException` |

Ocorrências perdidas (nenhum servidor no ar) disparam uma vez na volta. Uma ocorrência não espera a anterior terminar.

```csharp
sealed record RecurringJobOptions
{
    TimeZoneInfo? TimeZone { get; init; }   // padrão UTC
    int? MaxAttempts { get; init; }
    TimeSpan? RetryDelay { get; init; }
    TimeSpan? RetryJitter { get; init; }
    string? Queue { get; init; }
    int? Priority { get; init; }
}
sealed record RecurringJob(string Id, string JobName, string Cron, string TimeZone, JsonElement Args, int ArgsVersion,
    int MaxAttempts, TimeSpan RetryDelay, TimeSpan RetryJitter, string Queue, int Priority, DateTime NextRunAt,
    DateTime? LastRunAt, Guid? LastJobId, DateTime CreatedAt, DateTime UpdatedAt);
static class RecurringJobIds { static bool IsValid(string? id); } // ^[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}$
```

### `sealed class CronExpression`

| Membro | Contrato |
|---|---|
| `static CronExpression Parse(string expression)` | 5 campos ou 6 (segundos primeiro); `*`, `?`, listas, faixas, passos, nomes (`JAN`, `MON`), domingo `0`/`7`, macros `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`. `FormatException` se inválida; `ArgumentNullException` |
| `static bool TryParse(string expression, out CronExpression? cron)` | |
| `DateTime? GetNextOccurrence(DateTime fromUtc, TimeZoneInfo timeZone)` | Primeira ocorrência estritamente depois de `fromUtc`, em UTC; `null` se nunca casa (busca 30 anos). Horários no buraco do horário de verão são pulados |
| `string Expression { get; }`, `bool HasSeconds { get; }`, `ToString()` | |

## Exceções

| Tipo | Propriedades | Lançada por |
|---|---|---|
| `JobNotFoundException` | `Guid JobId` | `GetAsync`, `DeleteAsync`, `RequeueAsync`, `CancelAsync` |
| `InvalidJobTransitionException` | `Guid JobId`, `JobState CurrentState` | Delete/requeue/cancel em estado que não permite, ou mudança concorrente |
| `IdempotencyKeyConflictException` | `Guid JobId`, `string IdempotencyKey`, `Guid HolderJobId` | `RequeueAsync` quando outro job não finalizado está com a chave |
| `RecurringJobNotFoundException` | `string RecurringJobId` | `IRecurringJobClient.GetAsync`/`RemoveAsync`/`TriggerAsync` |

## Estados e transições

```csharp
enum JobState { Scheduled, Enqueued, Processing, Succeeded, Failed, Deleted, Cancelled }
enum AttemptOutcome { Succeeded, Failed, Interrupted, Cancelled }
```

`static class JobStateWire`:

| Membro | Contrato |
|---|---|
| `string ToWire(this JobState)` / `string ToWire(this AttemptOutcome)` | Nome em minúsculas |
| `JobState ParseState(string)` / `AttemptOutcome ParseOutcome(string)` | Sem diferenciar maiúsculas; `ArgumentException` se desconhecido |
| `IReadOnlyList<JobState> Finished` | `Succeeded`, `Failed`, `Deleted`, `Cancelled` (sujeitos à retenção) |
| `bool CanDelete(this JobState)` / `CanCancel` / `CanRequeue` | Se a operação aceita o estado |

`sealed record JobTransition(string Operation, IReadOnlyList<JobState> From, JobState To, string Reason, bool ResetAttempts = false, bool RefundAttempt = false)`,
com `bool Allows(JobState)`. Transições:

| Membro | De → para | Motivo |
|---|---|---|
| `Create(bool scheduled)` | (novo) → `Scheduled` / `Enqueued` | `scheduled_at_creation` / `created` |
| `Promote` | `Scheduled` → `Enqueued` | `became_due` |
| `Claim` | `Enqueued` → `Processing` | `claimed` |
| `Succeed` | `Processing` → `Succeeded` | `succeeded` |
| `Fail(bool retry)` | `Processing` → `Scheduled` / `Failed` | `retry_scheduled` / `max_attempts_exhausted` |
| `Recover(int attempts, int maxAttempts)` | `Processing` → `Enqueued` / `Failed` | `recovered_after_crash` / `max_attempts_exhausted` |
| `Interrupt` | `Processing` → `Enqueued` (`RefundAttempt`) | `interrupted_by_shutdown` |
| `Cancel` | `Scheduled`, `Enqueued`, `Processing` → `Cancelled` | `cancelled_by_user` |
| `Delete` | `Scheduled`, `Enqueued` → `Deleted` | `deleted_by_user` |
| `Requeue` | estados finalizados → `Enqueued` (`ResetAttempts`) | `requeued_by_user` |

`static class Reasons` traz esses motivos como constantes: `Created`, `ScheduledAtCreation`, `BecameDue`, `Claimed`,
`Succeeded`, `RetryScheduled`, `MaxAttemptsExhausted`, `RecoveredAfterCrash`, `InterruptedByShutdown`, `DeletedByUser`,
`RequeuedByUser`, `CancelledByUser`.

## Health check (`BackgroundJobs.Health`)

`sealed class JobServerHealthCheck : IHealthCheck`, registrado por `AddBackgroundJobs` (use com
`AddHealthChecks().AddCheck<JobServerHealthCheck>("jobs")`).

| Membro | Contrato |
|---|---|
| `bool IsReady { get; }` | O servidor migrou/recuperou e não recebeu pedido de parada |
| `JobServerHealthReport Report()` | Retrato atual |
| `Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)` | Unhealthy: não está pronto, ou `MaxAbandonedJobs` jobs sem worker. Degraded: fila parada, fila órfã, breaker aberto ou job sem worker há mais que `AbandonedJobAlertAfter` |

```csharp
sealed record JobServerHealthReport(HealthStatus Status, bool Ready, QueueHealth Queue,
    IReadOnlyList<CircuitBreakerHealth> CircuitBreakers, AbandonedHealth Abandoned, DateTimeOffset At);
sealed record QueueHealth(long? Ready, TimeSpan? OldestReadyAge, DateTimeOffset? SampledAt, bool Stalled,
    TimeSpan StalledThreshold, IReadOnlyList<QueueDepthHealth> Queues);
sealed record QueueDepthHealth(string Queue, bool Consumed, long Ready, TimeSpan OldestReadyAge, bool Orphaned);
sealed record CircuitBreakerHealth(string Job, string State, int ConsecutiveFailures, DateTimeOffset? OpenUntil); // closed|open|half-open
sealed record AbandonedHealth(IReadOnlyList<AbandonedJobHealth> Jobs, int Limit, bool Full, bool Alert, TimeSpan AlertAfter);
sealed record AbandonedJobHealth(Guid Id, string Name, DateTimeOffset Since, TimeSpan Age);
```

## Telemetria

`static class BackgroundJobsTelemetry { const string Name = "BackgroundJobs"; }`: nome do `ActivitySource` e do `Meter`.
Span `job <name>` por tentativa. Métricas: `backgroundjobs.attempt.duration` (histogram, s),
`backgroundjobs.attempts` (counter), `backgroundjobs.queue.depth` e `backgroundjobs.queue.oldest_age` (gauges por
`queue`), `backgroundjobs.jobs.abandoned` (gauge, por nó).

## SPI de store (`BackgroundJobs.Storage`)

Só para quem implementa um store novo; aplicações usam `IJobClient`. O contrato detalhado de cada método está nos
comentários de `src/BackgroundJobs/Storage/IJobStore.cs` e é verificado por `tests/BackgroundJobs.StoreContract`.

| Tipo | Papel |
|---|---|
| `interface IJobStore` | `MigrateAsync`, `InsertAsync`, `InsertManyAsync`, `GetAsync`, `ListAsync`, `NextRunAtAsync`, `PromoteDueAsync`, `ClaimAsync`, `RegisterConsumerAsync`, `FinishAsync`, `RenewLeasesAsync`, `CancelAsync`, `RecoverAsync`, `QueueStatsAsync`, `ApplyAsync`, `UpsertRecurringAsync`, `GetRecurringAsync`, `ListRecurringAsync`, `RemoveRecurringAsync`, `DueRecurringAsync`, `NextRecurringAtAsync`, `FireRecurringAsync`, `PurgeFinishedAsync` |
| `interface IJobStoreListener` | `Task RunAsync(CancellationToken ct)`: acorda os loops quando outro nó muda o store |
| `interface IJobSignals` | `void JobsReady()`, `void SchedulesChanged()` |
| `static class JobSignalsExtensions` | `void Entered(this IJobSignals, JobState)` |
| `sealed record NewJob(Guid Id, string Name, string ArgsJson, int ArgsVersion, int MaxAttempts, int RetryDelayMs, int RetryJitterMs, DateTime? RunAt, DateTime Now, string Queue, int Priority, string? IdempotencyKey = null)` | `JobTransition Creation` |
| `sealed record ClaimedJob(Guid Id, string Name, string ArgsJson, int ArgsVersion, int Attempt, int MaxAttempts, int RetryDelayMs, int RetryJitterMs, int Version, Guid AttemptId, int Run, int RefundedAttempts)` | `int CountedAttempts` |
| `sealed record ClaimRequest(string Queue, ClaimOrder Order, string NodeId, DateTime Now, TimeSpan LeaseDuration, int Limit, IReadOnlyCollection<string> ExcludedNames, IReadOnlyDictionary<string, int> ConcurrencyLimits)` | |
| `enum ClaimOrder { Priority, Fifo }` | |
| `sealed record LeaseRenewal(IReadOnlyCollection<Guid> Renewed, IReadOnlyCollection<Guid> CancelRequested)` | |
| `sealed record QueueStats(string Queue, long Ready, DateTime? OldestReadyAt, bool HasConsumer)` | |
| `sealed record RecurringDefinition(string Id, string JobName, string Cron, string TimeZone, string ArgsJson, int ArgsVersion, int MaxAttempts, int RetryDelayMs, int RetryJitterMs, string Queue, int Priority, DateTime NextRunAt, DateTime Now)` | |
| `sealed record RecurringFire(RecurringJob Definition, DateTime? Occurrence, DateTime? NextRunAt, NewJob Job)` | |
| `enum RecurringFireResult { Fired, Stale, Gone }` | |

## `BackgroundJobs.Postgres`

```csharp
static BackgroundJobsBuilder UsePostgres(this BackgroundJobsBuilder builder, string connectionString)
```
Connection string no formato Npgsql (`Host=...;Database=...`); URLs `postgres://` não são aceitas. `ArgumentException`
se nula ou em branco. As migrations (`src/BackgroundJobs.Postgres/Migrations/NNN_*.sql`, tabela `bj_migrations`) são
aplicadas na partida sob advisory lock. Notificações no canal `bj_jobs`.

### Enfileiramento transacional (outbox): `static class PostgresJobClientExtensions`

```csharp
static Task<Guid> EnqueueAsync<TArgs>(this IJobClient client, NpgsqlTransaction transaction, string jobName, TArgs args,
    JobOptions? options = null, CancellationToken ct = default)
static Task<IReadOnlyList<Guid>> EnqueueManyAsync(this IJobClient client, NpgsqlTransaction transaction,
    IEnumerable<JobToEnqueue> jobs, CancellationToken ct = default)
```
Grava os jobs na transação da aplicação: eles só existem se ela fizer commit; rollback não deixa nada (e libera a
`IdempotencyKey`). Mesma validação e mesmo retorno de `EnqueueAsync`/`EnqueueManyAsync`. A transação precisa estar aberta
no mesmo banco de `UsePostgres`. Os workers acordam pelo `NOTIFY bj_jobs`, que o Postgres entrega no commit (em todos os
nós, inclusive este) e descarta no rollback; antes do commit nada roda. Chave já usada por um job criado antes na mesma
transação devolve esse job; chave inserida por outra transação ainda aberta faz o insert esperar por ela. Use
`READ COMMITTED` (padrão do Postgres): em `REPEATABLE READ`/`SERIALIZABLE` esse conflito vira erro de serialização que a
aplicação precisa repetir. `InvalidOperationException` se o cliente não usa o store Postgres ou se a transação já terminou.

## `BackgroundJobs.AspNetCore` (API HTTP)

```csharp
static IEndpointRouteBuilder MapBackgroundJobsApi(this IEndpointRouteBuilder app)
static IResult ManagementEndpoints.Error(int status, string message) // {"error": message}
```

Erros têm sempre o formato `{"error":"mensagem"}`. Um `{id}` de job que não é Guid responde `404`. Não há rota para
criar jobs: o enqueue é feito pela aplicação via `IJobClient`. Não há autenticação: proteja as rotas.

| Rota | Sucesso | Erros |
|---|---|---|
| `GET /health` | `200 {"status":"ok"}` | `503 {"status":"unavailable"}` |
| `GET /health/details` | `200` (healthy ou degraded): `status`, `ready`, `queue{ready, oldestReadyAgeSeconds, stalled, stalledThresholdSeconds, sampledAt, queues[{name, consumed, orphaned, ready, oldestReadyAgeSeconds}]}`, `circuitBreakers[{job, state, consecutiveFailures, openUntil}]`, `abandoned{count, limit, full, alert, alertAfterSeconds, jobs[{id, name, since, ageSeconds}]}` | `503` com o mesmo corpo quando unhealthy |
| `GET /api/jobs?after=<id>&limit=<1..1000>` | `200 {"items":[{id, name, state, attempts, queue, priority}], "nextCursor": "<id>"\|null}`. Ordem crescente de id, `limit` padrão 1000, `nextCursor` só com a página cheia | `400` (`after` não é Guid, `limit` fora da faixa) |
| `GET /api/jobs/{id}` | `200 {id, name, args, state, attempts, maxAttempts, queue, priority, idempotencyKey, retryDelayMs, scheduledAt, createdAt, updatedAt, history[{state, at, reason}], attemptHistory[{attempt, startedAt, finishedAt, outcome, error}]}` | `404` |
| `DELETE /api/jobs/{id}` | `200 {"id","state":"deleted"}` | `404`, `409` |
| `POST /api/jobs/{id}/requeue` | `202 {"jobId"}` | `404`, `409` (estado, ou chave de idempotência em uso) |
| `POST /api/jobs/{id}/cancel` | `202 {"jobId"}` | `404`, `409` |
| `GET /api/recurring` | `200 {"items":[recorrente]}` | |
| `GET /api/recurring/{id}` | `200` recorrente | `404` |
| `PUT /api/recurring/{id}` | `200` recorrente. Corpo `{"job", "cron", "args":{...}, "timeZone"?, "options"?:{"queue", "priority", "maxAttempts", "retryDelayMs", "retryJitterMs"}}` | `400`: JSON inválido, `job`/`cron` ausentes, `args` não é objeto, fuso desconhecido, qualquer `ArgumentException` do upsert |
| `DELETE /api/recurring/{id}` | `200 {"id"}` | `404` |
| `POST /api/recurring/{id}/trigger` | `202 {"jobId"}` | `404` |

Recorrente no JSON: `id, job, cron, timeZone, args, queue, priority, maxAttempts, retryDelayMs, retryJitterMs, nextRunAt,
lastRunAt, lastJobId, createdAt, updatedAt`. Estados e resultados em minúsculas; datas ISO-8601 UTC.

## `BackgroundJobs.Dashboard`

```csharp
static RouteGroupBuilder MapBackgroundJobsDashboard(this IEndpointRouteBuilder app, string prefix = "/jobs",
    Action<DashboardOptions>? configure = null)
sealed class DashboardOptions { TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Utc; } // fuso até o visitante escolher
```
HTML renderizado no servidor, fora da descrição OpenAPI. Não há autenticação: use `.RequireAuthorization(...)` no grupo
retornado. POSTs cross-site (`Sec-Fetch-Site`) recebem `403`.

| Rota (sob `prefix`) | Função |
|---|---|
| `GET /` | Lista (50 por página, mais novos primeiro; `?state=`, `?after=`, `?dir=asc`, `?refresh=off`). `400` para estado desconhecido |
| `GET /{id}` | Detalhe, histórico, tentativas, args |
| `POST /{id}/delete`, `/{id}/requeue`, `/{id}/cancel` | Ação e redirect para o detalhe com mensagem |
| `GET /new`, `GET /new/{name}`, `POST /new/{name}` | Formulário gerado a partir do tipo de args para enfileirar um job registrado; `404` para nome não registrado |
| `POST /preferences` | Grava tema e fuso em cookies |
