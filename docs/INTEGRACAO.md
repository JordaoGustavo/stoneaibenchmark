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
10. [Health check](#10-health-check)
11. [Banco de dados](#11-banco-de-dados)
12. [Operação: múltiplos nós, crash e shutdown](#12-operação-múltiplos-nós-crash-e-shutdown)
13. [Limitações e boas práticas](#13-limitações-e-boas-práticas)

---

## 1. Requisitos

- .NET 10 (`net10.0`; SDK fixado em `global.json`: `10.0.402`, `rollForward: latestFeature`)
- PostgreSQL (testado com `postgres:17-alpine`)
- Dependências trazidas pela biblioteca: `Npgsql` 9.0.3, `Dapper` 2.1.66 e `Microsoft.AspNetCore.App` (framework reference)

Adicione a referência ao projeto:

```xml
<ProjectReference Include="path/to/src/BackgroundJobs/BackgroundJobs.csproj" />
```

Todos os tipos públicos ficam no namespace `BackgroundJobs` (o health check está em `BackgroundJobs.Health`).

## 2. Início rápido

```csharp
using BackgroundJobs;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddBackgroundJobs(
    o => o.ConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=backgroundjobs",
    jobs => jobs.Register<SendEmailJob, SendEmailArgs>("send-email"));

var app = builder.Build();
app.MapBackgroundJobsApi();        // opcional: /health e /api/jobs
app.MapBackgroundJobsDashboard();  // opcional: UI em /jobs
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
public sealed record JobContext(Guid JobId, int Attempt);
```

- **Lançar exceção = tentativa falhou.** A mensagem (ou o nome do tipo, se vazia) é registrada no histórico de tentativas.
- **Argumentos** são serializados com `System.Text.Json` (`JsonSerializerDefaults.Web`: camelCase, case-insensitive) e guardados como `jsonb`. Use records simples e evite mudanças incompatíveis no formato enquanto houver jobs pendentes.
- **Injeção de dependência:** cada execução abre um scope de DI novo e o job é criado com `ActivatorUtilities`, então **não precisa registrar o job no container** — apenas suas dependências. Serviços *scoped* (ex.: `DbContext`) funcionam.
- `context.Attempt` começa em 1; útil para lógica de idempotência ou logs.
- Não há `CancellationToken` em `ExecuteAsync` (ver [limitações](#13-limitações-e-boas-práticas)).

### Registro

```csharp
jobs.Register<TJob, TArgs>(string name, Action<JobRegistrationOptions>? configure = null)
```

- `name` é a chave estável persistida no banco. **Não renomeie** um job com instâncias pendentes.
- Nome vazio ou duplicado lança `ArgumentException`.
- Um job enfileirado com nome não registrado falha na execução com erro `"unknown job"` (consome tentativas normalmente).

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
record Job(Guid Id, string Name, JsonElement Args, JobState State, int Attempts, int MaxAttempts,
           TimeSpan RetryDelay, DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt,
           IReadOnlyList<JobHistoryEntry> History, IReadOnlyList<JobAttempt> AttemptHistory);
record JobHistoryEntry(JobState State, DateTime At, string Reason);
record JobAttempt(int Attempt, DateTime StartedAt, DateTime? FinishedAt, AttemptOutcome? Outcome, string? Error);
```

Datas em UTC (precisão de microssegundos, igual ao Postgres).

## 5. Configuração

`AddBackgroundJobs(Action<BackgroundJobsOptions> configure, Action<JobRegistry>? registerJobs = null)`

| Opção | Padrão | Descrição |
|---|---|---|
| `ConnectionString` | — (**obrigatória**) | Connection string Npgsql (`Host=...;Database=...`) |
| `WorkerCount` | `4` | Máximo de jobs simultâneos neste processo (≥ 1) |
| `NodeId` | `Environment.MachineName` | Identidade do processo como dono do lease. **Único por processo** |
| `LeaseDuration` | `30s` | Tempo até outro nó considerar o job abandonado (> 0) |
| `PollInterval` | `250ms` | Intervalo máximo de polling da fila e do agendador |
| `DefaultMaxAttempts` | `3` | Padrão de `JobOptions.MaxAttempts` |
| `DefaultRetryDelay` | `1s` | Padrão de `JobOptions.RetryDelay` |
| `DefaultRetryJitter` | `0` | Padrão de `JobOptions.RetryJitter` |

Configuração inválida lança `ArgumentException` na resolução das opções.

A biblioteca **não lê** `IConfiguration` sozinha; faça o bind você mesmo:

```csharp
builder.Services.AddBackgroundJobs(o =>
{
    o.ConnectionString = builder.Configuration.GetConnectionString("Jobs")!;
    o.WorkerCount = builder.Configuration.GetValue("Jobs:WorkerCount", 4);
    o.NodeId = Environment.GetEnvironmentVariable("NODE_ID") ?? Environment.MachineName;
}, jobs => ...);
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
```

| Operação | Estados de origem permitidos |
|---|---|
| Delete | `scheduled`, `enqueued` |
| Requeue | `succeeded`, `failed`, `deleted` |

Um job em `processing` **não pode ser deletado**. As transições usam concorrência otimista (`version`); operações concorrentes conflitantes resultam em `InvalidJobTransitionException`.

Resultados de tentativa (`AttemptOutcome`): `succeeded`, `failed`, `interrupted` (processo morreu durante a execução — conta como tentativa).

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

## 8. API HTTP (opcional)

`app.MapBackgroundJobsApi()` — expõe:

| Método e rota | Sucesso | Erros |
|---|---|---|
| `GET /health` | `200 {"status":"ok"}` | `503 {"status":"unavailable"}` |
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

`app.MapBackgroundJobsDashboard(string prefix = "/jobs")` — UI HTML renderizada no servidor (sem JavaScript), com modo escuro.

- `GET {prefix}/` — lista paginada (50 por página), filtro por estado, auto-refresh a cada 5 s (desligável com `?refresh=off`).
- `GET {prefix}/{id}` — detalhes, histórico, tentativas com duração e erro, args em JSON; botões Requeue/Delete quando a transição é válida.
- `POST {prefix}/{id}/delete` e `POST {prefix}/{id}/requeue` — ações via formulário.

Segurança embutida: CSP restritiva, `nosniff`, `no-referrer`, `no-store` e rejeição (`403`) de POSTs cross-site via `Sec-Fetch-Site`. **Não há autenticação embutida** — recomendado:

```csharp
app.MapBackgroundJobsDashboard().RequireAuthorization("admin");
```

> O filtro por estado varre a lista em ordem de id (até 10.000 jobs por página) por não haver índice por estado; em bases muito grandes pode ser lento.

## 10. Health check

`JobServerHealthCheck` (registrado automaticamente) é de **readiness**: saudável apenas depois de migrar/recuperar e antes do pedido de parada.

```csharp
builder.Services.AddHealthChecks().AddCheck<JobServerHealthCheck>("jobs");
app.MapHealthChecks("/ready");
```

Ou use o `/health` de `MapBackgroundJobsApi()`.

## 11. Banco de dados

- Migrations embutidas são aplicadas **automaticamente** na partida, numa transação protegida por advisory lock (seguro com vários nós subindo juntos). Controle em `bj_migrations`.
- Tabelas: `bj_jobs`, `bj_attempts`, `bj_history` (+ sequence `bj_enqueue_seq`). O usuário do banco precisa de permissão de `CREATE` no schema na primeira execução.
- **Não há limpeza automática** de jobs antigos. Se o volume for alto, crie uma rotina própria (ex.: apagar `succeeded` antigos — `bj_attempts`/`bj_history` referenciam `bj_jobs` por FK).

## 12. Operação: múltiplos nós, crash e shutdown

- **Múltiplos nós:** suportado. Cada processo deve ter `NodeId` **único** — na partida, um nó recupera imediatamente todos os jobs presos com o seu próprio `NodeId`. Dois processos com o mesmo id roubariam jobs um do outro.
- **Lease e heartbeat:** cada job em execução tem um lease de `LeaseDuration`, renovado a cada `LeaseDuration / 3`. Se o nó morrer, outro nó recupera o job após o lease expirar; a tentativa vira `interrupted` e conta para `MaxAttempts`.
- **Reinício com mesmo `NodeId`:** recupera os jobs desse nó na hora, sem esperar o lease.
- **Parada graciosa:** ao parar, o servidor deixa de pegar novos jobs e **espera os em execução terminarem, sem limite de tempo** (o HTTP continua aceitando enqueues durante o drain). Ajuste o timeout do host conforme sua plataforma:

  ```csharp
  builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = Timeout.InfiniteTimeSpan);
  ```

  Em Kubernetes/ECS, alinhe `terminationGracePeriodSeconds` com a duração dos seus jobs mais longos.
- **Partida:** `StartAsync` retorna imediatamente; migração e recuperação são refeitas em background até o banco responder.

## 13. Limitações e boas práticas

- **Entrega at-least-once:** um job pode executar mais de uma vez (crash, ou banco indisponível por mais que `LeaseDuration` durante a execução). **Escreva jobs idempotentes.**
- **Sem cancelamento:** jobs não recebem `CancellationToken`; jobs longos atrasam o shutdown.
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
| API HTTP | `src/BackgroundJobs/Http/ManagementEndpoints.cs` |
| Dashboard | `src/BackgroundJobs/Http/DashboardEndpoints.cs` |
| Schema | `src/BackgroundJobs/Storage/Postgres/Migrations/001_init.sql` |
| Exemplo sem HTTP | `samples/WorkerExample/Program.cs` |
| Exemplo com HTTP | `src/BackgroundJobs.Demo/Program.cs` |
