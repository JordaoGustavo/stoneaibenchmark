# Guia de Integração — BackgroundJobs

Biblioteca .NET para execução de jobs em segundo plano persistidos em PostgreSQL: agendamento, retry, FIFO, múltiplos workers, recuperação de crash e parada graciosa.

> Para rodar o demo e os testes, veja [`RUNNING.md`](../RUNNING.md). A especificação original está em [`docs/SPEC.md`](SPEC.md).

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
| `BackgroundJobs.AspNetCore` | `MapBackgroundJobsApi()`: `/api/jobs`, `/api/recurring`, `/health`, `/health/details` | `BackgroundJobs`, ASP.NET Core |
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
- **`context.CancellationToken`** dispara quando a tentativa estoura o timeout, quando um usuário cancela o job ou quando o servidor está parando. Repasse-o para `HttpClient`, EF, `Task.Delay` etc. O cancelamento é cooperativo:
  - **Timeout** (`DefaultJobTimeout` ou `JobRegistrationOptions.Timeout`): se o job parar (com qualquer exceção), a tentativa falha com erro `timed out after N ms` e segue a política de retry.
  - **Cancelamento** (`IJobClient.CancelAsync`, `POST /api/jobs/{id}/cancel`, botão no dashboard): o job termina em `cancelled`, sem retry — também se a tentativa falhar, estourar o timeout ou for interrompida antes de o pedido chegar ao nó. Só um sucesso prevalece sobre o pedido.
  - **Job que ignora o token** (timeout ou cancelamento): depois de `TimeoutGracePeriod` o worker é liberado para outros jobs, mas o job **continua em `processing`**, com o lease renovado, até o código voltar; aí o resultado real é registrado (inclusive sucesso). Como o job não sai de `processing`, nenhum retry roda em paralelo, em nenhum nó. No máximo `MaxAbandonedJobs` jobs ficam assim; além disso, o job segura o worker. **Um job que nunca volta fica em `processing` para sempre** e ocupa uma dessas vagas; com as vagas esgotadas, os próximos jobs travados seguram workers. O [health check](#10-health-check-e-observabilidade) avisa: `degraded` quando um job está assim há mais que `AbandonedJobAlertAfter` (padrão 5 min), `unhealthy` quando as vagas acabam (reiniciar o processo as libera). A métrica `backgroundjobs.jobs.abandoned` conta esses jobs.
  - **Parada do servidor:** se o job parar, a tentativa vira `interrupted` e o job volta para `enqueued` na hora (`interrupted_by_shutdown`), sem esperar o lease e **sem gastar tentativa**: a tentativa conta em `Job.RefundedAttempts` e o orçamento é `Attempts - RefundedAttempts`. Cada execução mantém seu próprio número (1, 2, 3…), então `context.Attempt` nunca se repete. Pela API HTTP, as tentativas devolvidas são as entradas `interrupted_by_shutdown` do histórico; o dashboard mostra "1 of 1 (+1 interrupted by shutdown)". Jobs que ignoram o token são esperados até `ShutdownTimeout` (infinito por padrão); depois são registrados do mesmo jeito e o processo encerra.

  ```csharp
  jobs.Register<ReportJob, ReportArgs>("report", o => o.Timeout = TimeSpan.FromMinutes(2));
  ```

### Registro

```csharp
jobs.Register<TJob, TArgs>(string name, Action<JobRegistrationOptions>? configure = null)
```

- `name` é a chave estável persistida no banco. **Não renomeie** um job com instâncias pendentes.
- Nome vazio ou duplicado lança `ArgumentException`.
- Cada nó só faz claim de jobs cujo nome **ele** registrou. Um job com nome que nenhum nó vivo registrou (nome com erro
  de digitação, ou um job novo durante um deploy gradual em que só os nós novos o têm) fica em `enqueued`, sem gastar
  tentativas, até subir um nó que o registre; enquanto isso a fila aparece como parada em `/health/details` (veja
  [Health check](#10-health-check-e-observabilidade)). O erro `unknown job '<nome>'` na execução continua existindo só como rede de segurança.
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
| `EnqueueAsync<TArgs>(name, args, JobOptions? options = null, ct)` | Persiste e retorna o `Guid` (UUIDv7). Com `IdempotencyKey` já usada por um job não terminado, não cria nada e retorna o id dele | `ArgumentException` (nome vazio, opções inválidas) |
| `GetAsync(id, ct)` | Retorna o `Job` completo, com histórico e tentativas | `JobNotFoundException` |
| `ListAsync(Guid? after = null, int limit = 100, bool descending = false, ct)` | Resumos em ordem de criação (mais novos primeiro com `descending: true`); paginação por cursor (`after` = último id visto). `limit` 1..1000 | `ArgumentException` |
| `DeleteAsync(id, ct)` | Remove da fila (só `scheduled`/`enqueued`) | `JobNotFoundException`, `InvalidJobTransitionException` |
| `RequeueAsync(id, ct)` | Reexecuta (só `succeeded`/`failed`/`deleted`/`cancelled`), zera tentativas, mantém histórico | `JobNotFoundException`, `InvalidJobTransitionException`, `IdempotencyKeyConflictException` (a chave do job está com outro job não terminado) |
| `CancelAsync(id, ct)` | Para de vez (`scheduled`/`enqueued` na hora; `processing` dispara o token — na hora neste nó, em até `LeaseDuration / 3` em outro — e termina em `cancelled`) | `JobNotFoundException`, `InvalidJobTransitionException` |

Erros de banco em chamadas do `IJobClient` **propagam para quem chamou** (não há retry automático no cliente).

### `JobOptions` (por job)

Campos não informados usam os padrões de `BackgroundJobsOptions`.

| Campo | Significado |
|---|---|
| `RunAt` (`DateTimeOffset?`) | Não executar antes desse instante. Nulo ou no passado = agora |
| `MaxAttempts` (`int?`) | Total de execuções, incluindo a primeira (≥ 1) |
| `RetryDelay` (`TimeSpan?`) | Espera fixa entre tentativas (≥ 0) |
| `RetryJitter` (`TimeSpan?`) | Atraso aleatório extra em `[0, RetryJitter]` somado a cada retry |
| `Queue` (`string?`) | Fila do job (padrão `"default"`). 1–64 caracteres `a-z`, `0-9`, `-`, `_`, `.`, começando por letra ou dígito (`JobQueues.IsValidName`) |
| `Priority` (`int?`) | Maior roda primeiro dentro da fila; de `-100` a `100`, padrão `0` (`JobQueues`) |
| `IdempotencyKey` (`string?`) | Deduplicação: enquanto um job com essa chave não terminou (`awaiting`, `scheduled`, `enqueued`, `processing`), enfileirar a mesma chave retorna o id dele sem criar outro (nome, args e opções da nova chamada são ignorados), mesmo com chamadas concorrentes em vários nós. Depois que ele termina (`succeeded`, `failed`, `deleted`, `cancelled`) a chave fica livre. Até 200 caracteres, não vazia |

| `ContinueAfter` (`Guid?`) | Continuação: o job só roda depois que este job pai finalizar (veja abaixo) |
| `Condition` (`ContinuationCondition?`) | Com `ContinueAfter`: `OnSucceeded` (padrão) ou `OnFinished` |

Agendamento não tem método próprio — é `EnqueueAsync` com `RunAt`:

```csharp
await client.EnqueueAsync("greet", new GreetArgs("em 2s"), new JobOptions
{
    RunAt = DateTimeOffset.UtcNow.AddSeconds(2),
    MaxAttempts = 5,
    RetryDelay = TimeSpan.FromMilliseconds(500),
});
```

### Continuações (rodar B depois de A)

```csharp
var a = await client.EnqueueAsync("import", new ImportArgs(file));
var b = await client.EnqueueAsync("notify", new NotifyArgs(file), new JobOptions { ContinueAfter = a });
await client.EnqueueAsync("cleanup", new CleanupArgs(file),
    new JobOptions { ContinueAfter = a, Condition = ContinuationCondition.OnFinished });
```

- A continuação fica em `awaiting` enquanto o pai não finaliza. Retries do pai não a afetam.
- Quando o pai entra num estado final, **na mesma transação**, cada continuação vai para `enqueued` (ou `scheduled`, se o
  `RunAt` dela ainda estiver no futuro) se a condição foi atendida, ou para `cancelled` (motivo
  `continuation_condition_not_met`) se não. `OnSucceeded` exige `succeeded`; `OnFinished` aceita `succeeded`, `failed`,
  `deleted` e `cancelled`. O cancelamento propaga pela cadeia (A → B → C: A falha, B e C `OnSucceeded` são cancelados).
- Pai já finalizado no enqueue: a decisão é imediata. Pai inexistente (ou já purgado pela retenção):
  `JobNotFoundException`.
- Sem corrida entre nós: o enqueue da continuação trava a linha do pai (`FOR SHARE`) e a finalização do pai trava as
  continuações, então ou o enqueue vê o pai finalizado, ou a finalização vê a continuação.
- Um pai só. Para "depois de A e B", encadeie (B depois de A, C depois de B) ou use um job que enfileire os demais.
- Requeue do pai não roda de novo continuações já liberadas ou canceladas; continuações enfileiradas depois do requeue
  esperam a nova execução. Requeue de uma continuação cancelada a roda como job comum.
- Uma continuação `awaiting` pode ser deletada ou cancelada; ela segura sua `IdempotencyKey`.
- Retenção: só jobs finalizados são purgados, e um pai finalizado já não tem continuações `awaiting`; por isso o id do
  pai em `ContinueAfter` pode apontar para um job que não existe mais.
- O health check não conta jobs `awaiting` como fila parada: eles não estão prontos.

### Modelos retornados

```csharp
record JobSummary(Guid Id, string Name, JobState State, int Attempts, DateTime? ScheduledAt, DateTime CreatedAt,
                  DateTime UpdatedAt, string Queue, int Priority);
record Job(Guid Id, string Name, JsonElement Args, int ArgsVersion, JobState State, int Attempts, int MaxAttempts,
           TimeSpan RetryDelay, DateTime? ScheduledAt, DateTime CreatedAt, DateTime UpdatedAt,
           IReadOnlyList<JobHistoryEntry> History, IReadOnlyList<JobAttempt> AttemptHistory,
           int RefundedAttempts, string Queue, int Priority, string? IdempotencyKey);
record JobHistoryEntry(JobState State, DateTime At, string Reason);
record JobAttempt(int Attempt, DateTime StartedAt, DateTime? FinishedAt, AttemptOutcome? Outcome, string? Error);
```

Datas em UTC (precisão de microssegundos, igual ao Postgres).

### Jobs recorrentes (`IRecurringJobClient`)

Uma definição recorrente tem um id estável e enfileira um job comum (com retry, fila, prioridade e histórico) a cada
ocorrência de uma expressão cron. Declare no código — a definição é criada/atualizada a cada partida:

```csharp
builder.Services.AddBackgroundJobs(o => { /* ... */ }, jobs => jobs.Register<ReportJob, ReportArgs>("daily-report"))
    .UsePostgres(connectionString)
    .AddRecurringJob("daily-report", "daily-report", "0 9 * * MON-FRI", new ReportArgs("vendas"),
        new RecurringJobOptions { TimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/Sao_Paulo"), Queue = "reports" });
```

ou em tempo de execução, via `IRecurringJobClient`:

| Método | Efeito | Erros |
|---|---|---|
| `UpsertAsync<TArgs>(id, jobName, cron, args, RecurringJobOptions?, ct)` | Cria ou substitui. Mantém a próxima ocorrência se cron e fuso não mudaram | `ArgumentException` (id, cron, fuso/opções inválidos, job não registrado ou cron que nunca dispara) |
| `GetAsync(id)` / `ListAsync()` | `RecurringJob` (`NextRunAt`, `LastRunAt`, `LastJobId`, ...) | `RecurringJobNotFoundException` |
| `RemoveAsync(id)` | Para as próximas ocorrências; jobs já criados ficam | `RecurringJobNotFoundException` |
| `TriggerAsync(id)` | Enfileira um job agora, sem mexer na próxima ocorrência | `RecurringJobNotFoundException` |

- **Cron:** 5 campos (`minuto hora dia mês dia-da-semana`) ou 6 com segundos na frente. `*`, `?`, listas, faixas, passos
  (`*/5`, `1-10/2`), nomes (`JAN`, `MON`), domingo = `0` ou `7`, e `@yearly`, `@monthly`, `@weekly`, `@daily`, `@hourly`.
  Com dia do mês e dia da semana restritos, vale qualquer um dos dois (cron clássico). Sem `L`, `W` ou `#`.
- **Fuso:** `RecurringJobOptions.TimeZone` (padrão UTC). Horários que caem no buraco do horário de verão são pulados; na
  hora repetida, crons que rodam toda hora (`*/5 * * * *`) seguem nas duas passagens e os demais rodam só na primeira.
- **Ocorrências perdidas** (nenhum servidor no ar): disparam **uma vez** quando um servidor volta, e a agenda segue a partir de agora.
- **Sem sobreposição controlada:** cada ocorrência é um job independente; uma não espera a anterior terminar.
- **Vários nós:** cada ocorrência gera exatamente um job (o disparo e o avanço de `next_run_at` são atômicos), sempre com os args e opções vigentes: um disparo que cruza com um upsert é refeito com a definição nova.
- **Job registrado:** ao contrário de `EnqueueAsync`, o nome do job precisa estar registrado no processo que faz o upsert.
  Com `UpsertAsync`, um job não registrado lança `ArgumentException`. Com `AddRecurringJob`, o erro **não** impede o host de subir: é só registrado em log (`recurring job {Id} was not declared`) e a definição não é criada. Confira o log após o deploy.
- Uma definição que um nó não consegue agendar (fuso desconhecido nele) é registrada em log uma vez e deixada para os outros nós, sem atrasar as demais.
- Ids: 1–100 caracteres `A-Z`, `a-z`, `0-9`, `-`, `_`, `.`, `:`. Remover `AddRecurringJob` do código não apaga a definição — use `RemoveAsync`.

## 5. Configuração

`AddBackgroundJobs(Action<BackgroundJobsOptions>? configure = null, Action<JobRegistry>? registerJobs = null)` devolve um
`BackgroundJobsBuilder`, no qual se escolhe **um** store: `.UsePostgres(connectionString)` (pacote `BackgroundJobs.Postgres`,
connection string Npgsql `Host=...;Database=...`) ou `.UseInMemoryStorage()`. Sem store, a aplicação falha ao iniciar com
`InvalidOperationException: no job store configured`.

| Opção | Padrão | Descrição |
|---|---|---|
| `FinishedJobRetention` | **obrigatório** | Quanto tempo um job finalizado (`succeeded`, `failed`, `deleted`, `cancelled`) é mantido, com histórico e tentativas. `Timeout.InfiniteTimeSpan` mantém para sempre |
| `RetentionSweepInterval` | `1min` | Frequência da limpeza de jobs finalizados |
| `RetentionBatchSize` | `1000` | Jobs removidos por comando de limpeza (a limpeza repete até esgotar) |
| `WorkerCount` | `4` | Workers da fila `default` quando `Queues` está vazio (≥ 1); ignorado caso contrário |
| `Queues` | vazio | Filas que este processo consome → workers de cada uma, ex. `{ ["default"] = 4, ["critical"] = 2 }`. Vazio = só `default` com `WorkerCount` |
| `NodeId` | `MachineName` + sufixo aleatório | Identidade do processo como dono do lease. Já é único por instância; se sobrescrever, **mantenha único por processo** |
| `LeaseDuration` | `30s` | Tempo até outro nó considerar o job abandonado (> 0) |
| `PollInterval` | `250ms` | Polling de fallback da fila (o caminho normal é LISTEN/NOTIFY do Postgres) |
| `SchedulerMaxWait` | `5s` | Espera máxima do agendador; normalmente dorme até o próximo `run_at` ou acorda por notificação |
| `FifoClaimEvery` | `10` | A cada N jobs pegos de uma fila, um é o mais antigo dela, ignorando a prioridade (anti-starvation). `null` = prioridade estrita; ≥ 2 |
| `ClaimBatchSize` | `null` | Máximo de jobs por claim, por fila; `null` = tantos quanto a fila tiver slots livres (nunca acima dos workers dela) |
| `DefaultMaxAttempts` | `3` | Padrão de `JobOptions.MaxAttempts` |
| `DefaultRetryDelay` | `1s` | Padrão de `JobOptions.RetryDelay` |
| `DefaultRetryJitter` | `0` | Padrão de `JobOptions.RetryJitter` |
| `DefaultJobTimeout` | `Timeout.InfiniteTimeSpan` | Tempo máximo de uma tentativa; `JobRegistrationOptions.Timeout` sobrescreve por tipo |
| `TimeoutGracePeriod` | `5s` | Após timeout ou cancelamento, quanto um job que ignora o token segura o worker |
| `MaxAbandonedJobs` | `4` | Máximo de jobs rodando sem worker depois da folga; acima disso seguram o worker |
| `AbandonedJobAlertAfter` | `5min` | Health check `degraded` quando um job roda sem worker há mais que isso |
| `ShutdownTimeout` | `Timeout.InfiniteTimeSpan` | Na parada, quanto esperar jobs que ignoram o token antes de devolvê-los à fila e encerrar |

Configuração inválida lança `ArgumentException` ao iniciar o host.

### Retenção

Jobs finalizados, seu histórico e suas tentativas crescem sem limite se ninguém os apagar, por isso
`FinishedJobRetention` **não tem padrão**: sem ela a aplicação não inicia. Cada nó varre a cada `RetentionSweepInterval`
e remove de vez os jobs em `succeeded`, `failed`, `deleted` ou `cancelled` cuja última transição é mais antiga que a retenção (depois
disso o `GET` devolve 404 e não dá mais para fazer requeue). Jobs `scheduled`, `enqueued` e `processing` nunca são
removidos. Um requeue reinicia a contagem. Para manter tudo (e cuidar da limpeza por conta própria), use
`Timeout.InfiniteTimeSpan`.

A biblioteca **não lê** `IConfiguration` sozinha; faça o bind você mesmo:

```csharp
builder.Services.AddBackgroundJobs(o =>
{
    o.FinishedJobRetention = builder.Configuration.GetValue<TimeSpan?>("Jobs:Retention");
    o.WorkerCount = builder.Configuration.GetValue("Jobs:WorkerCount", 4);
    o.NodeId = Environment.GetEnvironmentVariable("NODE_ID") ?? o.NodeId;
}, jobs => ...)
.UsePostgres(builder.Configuration.GetConnectionString("Jobs")!);
```

> URLs no formato `postgres://user:pass@host:5432/db` **não** são aceitas pela biblioteca; o projeto Demo tem um conversor (`ToConnectionString` em `src/BackgroundJobs.Demo/Program.cs`) que pode ser copiado.

## 6. Ciclo de vida do job

Estados (`JobState`, no JSON em minúsculas): `scheduled`, `enqueued`, `processing`, `succeeded`, `failed`, `deleted`, `cancelled`, `awaiting`.

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

 succeeded / failed / deleted / cancelled ── requeued_by_user ──► enqueued   (tentativas zeradas, histórico mantido)
 processing ── crash/lease expirado ──► enqueued (recovered_after_crash) ou failed se sem tentativas
 processing ── cancelado pela parada ──► enqueued (interrupted_by_shutdown), sempre, tentativa devolvida
 scheduled / enqueued / processing ── cancelled_by_user ──► cancelled   (processing: quando o código do job para)
 criação com ContinueAfter ── awaiting_parent ──► awaiting ── pai finalizou ──► enqueued / scheduled (parent_finished)
                                                                         └──► cancelled (continuation_condition_not_met)
```

| Operação | Estados de origem permitidos |
|---|---|
| Delete | `scheduled`, `enqueued`, `awaiting` |
| Requeue | `succeeded`, `failed`, `deleted`, `cancelled` |
| Cancel | `scheduled`, `enqueued`, `processing`, `awaiting` |

Um job em `processing` **não pode ser deletado**. As transições usam concorrência otimista (`version`); operações concorrentes conflitantes resultam em `InvalidJobTransitionException`.

Resultados de tentativa (`AttemptOutcome`): `succeeded`, `failed`, `interrupted` (processo morreu durante a execução — conta como tentativa; ou parou — não conta), `cancelled`.

Os motivos do histórico estão em constantes públicas na classe `Reasons`.

## 7. Retry, filas, prioridades, workers e circuit breaker

- **Retry:** atraso **fixo** (`RetryDelay` + jitter aleatório opcional), não exponencial. A tentativa falha volta a `scheduled` e é promovida quando vencer.
- **Filas:** cada job pertence a uma fila (`JobOptions.Queue`, padrão `default`), fixa por toda a vida dele (retries e requeues voltam para a mesma). Cada fila consumida (`Queues`) tem **seus próprios workers**: uma fila lotada de jobs lentos não atrasa outra. Uma fila que nenhum processo consome guarda seus jobs em `enqueued`; diferentes processos podem consumir filas diferentes do mesmo banco.
- **Ordem:** dentro da fila, maior `Priority` primeiro; empate em ordem FIFO de entrada em `enqueued` (retries, recuperações e requeues vão para o fim da sua prioridade). Com 1 worker na fila a ordem de execução é estrita. Para que um fluxo contínuo de prioridade alta não segure os de baixa para sempre, a cada `FifoClaimEvery` (padrão 10) jobs pegos de uma fila um é o mais antigo dela, qualquer que seja a prioridade; cargas que não podem esperar nada ficam melhor em filas próprias.
- **Workers:** um job só é reivindicado quando a sua fila tem slot livre (sem prefetch), então nunca rodam mais que os workers da fila por processo. Vários processos podem compartilhar o banco (`FOR UPDATE SKIP LOCKED`).
- **Limite de concorrência por tipo de job (opt-in):** `MaxConcurrency` limita quantos jobs daquele nome ficam em `processing` ao mesmo tempo **no cluster inteiro** (todos os nós e filas), não por processo. Os excedentes ficam em `enqueued` e os workers livres pegam os próximos jobs de outros tipos. Útil para proteger um recurso externo (API com rate limit, relatório pesado) ou para garantir uma execução por vez (`MaxConcurrency = 1`).

  ```csharp
  jobs.Register<ReportJob, ReportArgs>("report", o => o.MaxConcurrency = 2);
  ```

  Configure igual em todos os nós: um nó sem o limite pega esse tipo livremente. Jobs abandonados por timeout ainda contam até terminarem, e um job de um nó morto conta até a lease expirar e ele ser recuperado. No Postgres, claims de tipos limitados são serializados por um advisory lock por nome.
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
  e se zera tentativas vem pronto em `JobTransition` (`Promote`, `Claim`, `Succeed`, `Fail`, `Recover`, `Interrupt`, `Cancel`, `Delete`, `Requeue`),
  junto com `NewJob.Creation`. O store só aplica a transição de forma atômica, seguindo a mecânica comum descrita no
  comentário de `IJobStore`: guarda por estado de origem + versão (+ dono), incrementa a versão, limpa o lock, sequência
  FIFO ao entrar em `enqueued`, grava o histórico. `InsertAsync` deduplica por `NewJob.IdempotencyKey` (no máximo um job
  não terminado por chave, de forma atômica — no Postgres, um índice `UNIQUE` parcial) e retorna o id do job que ficou com a chave.
  `ClaimAsync(ClaimRequest)` recebe **uma fila** e uma ordem: `Priority` (prioridade desc, entrada, sequência) ou `Fifo`
  (entrada, sequência). Mantenha os prontos de cada fila indexados nas duas ordens, para o claim ler só o início de uma
  fila: no Postgres são os índices parciais `bj_jobs_ready` e `bj_jobs_ready_fifo` (o claim escreve `state = 'enqueued'`
  literal, senão o plano genérico de um statement preparado não usa índice parcial); num Redis, dois sorted sets por fila,
  um com score `(MaxPriority - priority) * 2^40 + sequência` (exato em `double` para as prioridades de `JobQueues`, que por
  isso são limitadas) e outro com score `sequência`.
  `ClaimRequest.ConcurrencyLimits` traz o `MaxConcurrency` por nome: o claim pula jobs de um nome que já tem o limite em
  `processing` e continua preenchendo com os seguintes, e claims concorrentes de nós diferentes juntos nunca passam do
  limite (no Postgres, `pg_advisory_xact_lock` por nome antes de contar). `ClaimRequest.Names`, quando não nulo, restringe
  o claim a esses nomes (o servidor passa os nomes registrados no nó); os demais são pulados como `ExcludedNames` e ficam
  em `enqueued`.
  `RegisterConsumerAsync` grava quais filas cada nó consome, com validade (num Redis, uma chave com TTL por nó e fila);
  `QueueStatsAsync` devolve uma entrada por fila com jobs prontos e se algum nó vivo a consome.
- **`IJobStoreListener`** (opcional) — acorda os loops quando *outro nó* muda o store (no Postgres, LISTEN/NOTIFY), via
  `IJobSignals`. As mudanças do próprio nó já acordam os loops pelo núcleo; sem listener, outros nós são vistos no polling.
- Um método de extensão `UseXxx(this BackgroundJobsBuilder)` que faz `Services.Replace` de `IJobStore` e `IJobStoreListener`
  (veja `src/BackgroundJobs.Postgres/PostgresBuilderExtensions.cs`).

**Conformidade:** os cenários E2E que não dependem de durabilidade (S01–S07, S11, S13, S17) rodam contra **todos** os stores
de `Stores.All` (`tests/BackgroundJobs.E2E/Infra/PostgresFixture.cs`). Para validar um store novo, adicione-o ali e ao
demo (`STORAGE=...`): o mesmo comportamento observável é o contrato. Cenários de restart/crash/parada (S08–S10) exigem
store durável.

## 8. API HTTP (opcional)

Pacote `BackgroundJobs.AspNetCore`. `app.MapBackgroundJobsApi()` — expõe:

| Método e rota | Sucesso | Erros |
|---|---|---|
| `GET /health` | `200 {"status":"ok"}` | `503 {"status":"unavailable"}` |
| `GET /health/details` | `200` relatório (healthy/degraded) — ver [§10](#10-health-check-e-observabilidade) | `503` quando não está processando |
| `GET /api/jobs?after=<id>&limit=<1..1000>` | `200 {"items":[{"id","name","state","attempts","queue","priority"}],"nextCursor":"<id>"\|null}` | `400` |
| `GET /api/jobs/{id}` | `200` job completo | `404` |
| `DELETE /api/jobs/{id}` | `200 {"id":"...","state":"deleted"}` | `404`, `409` |
| `POST /api/jobs/{id}/requeue` | `202 {"jobId":"..."}` | `404`, `409` (estado não permite, ou a chave de idempotência está com outro job) |
| `POST /api/jobs/{id}/cancel` | `202 {"jobId":"..."}` (um job em `processing` passa a `cancelled` quando seu código para) | `404`, `409` |

| `GET /api/recurring` | `200 {"items":[recorrente]}` | |
| `GET /api/recurring/{id}` | `200` recorrente | `404` |
| `PUT /api/recurring/{id}` | `200` recorrente. Corpo: `{"job","cron","args":{...},"timeZone"?,"options"?:{"queue","priority","maxAttempts","retryDelayMs","retryJitterMs"}}` | `400` |
| `DELETE /api/recurring/{id}` | `200 {"id":"..."}` | `404` |
| `POST /api/recurring/{id}/trigger` | `202 {"jobId":"..."}` | `404` |

- Erros sempre no formato `{"error":"mensagem"}`. Id que não é Guid → `404`.
- Recorrente: `id, job, cron, timeZone, args, queue, priority, maxAttempts, retryDelayMs, retryJitterMs, nextRunAt, lastRunAt, lastJobId, createdAt, updatedAt`.
- `limit` padrão 1000; `nextCursor` só vem preenchido quando a página está cheia.
- Job completo (ordem das chaves): `id, name, args, state, attempts, maxAttempts, queue, priority, idempotencyKey, retryDelayMs, scheduledAt, createdAt, updatedAt, history[{state,at,reason}], attemptHistory[{attempt,startedAt,finishedAt,outcome,error}]`. Datas ISO-8601 UTC com `Z`; `outcome` é `null` enquanto a tentativa roda.

A API **não cria jobs** — enfileiramento é responsabilidade da sua aplicação via `IJobClient` (o Demo mostra um exemplo em `POST /demo/jobs`). Para reutilizar o formato de erro: `ManagementEndpoints.Error(status, message)`.

**Proteja as rotas** se expostas publicamente:

```csharp
app.MapBackgroundJobsApi();  // retorna IEndpointRouteBuilder
```

Coloque-as atrás de autenticação/rede interna conforme sua infraestrutura.

## 9. Dashboard (opcional)

Pacote `BackgroundJobs.Dashboard`. `app.MapBackgroundJobsDashboard(string prefix = "/jobs")` — UI HTML renderizada no servidor (sem JavaScript), com modo escuro.

- `GET {prefix}/` — lista paginada (50 por página), filtro por estado, auto-refresh a cada 5 s (desligável com `?refresh=off`).
- `GET {prefix}/{id}` — detalhes, histórico, tentativas com duração e erro, args em JSON; botões Requeue/Delete/Cancel quando a transição é válida.
- `POST {prefix}/{id}/delete`, `POST {prefix}/{id}/requeue` e `POST {prefix}/{id}/cancel` — ações via formulário.
- `GET {prefix}/new`, `GET {prefix}/new/{name}` e `POST {prefix}/new/{name}` — **disparo de jobs**: lista os jobs registrados, gera um formulário a partir do tipo de args e enfileira com as opções informadas. Quem acessa o dashboard consegue enfileirar qualquer job registrado.
- `POST {prefix}/preferences` — tema e fuso horário do visitante, guardados em cookie.

Como o dashboard **enfileira jobs** (não é só leitura), proteja-o como uma API de escrita.

Segurança embutida: CSP restritiva, `nosniff`, `no-referrer`, `no-store` e rejeição (`403`) de POSTs cross-site via `Sec-Fetch-Site`. **Não há autenticação embutida** — recomendado:

```csharp
app.MapBackgroundJobsDashboard().RequireAuthorization("admin");
```

> O filtro por estado varre a lista em ordem de id (até 10.000 jobs por página) por não haver índice por estado; em bases muito grandes pode ser lento.

## 10. Health check e observabilidade

### Health check

`JobServerHealthCheck` (registrado automaticamente):

- **Unhealthy** fora do intervalo "migrou/recuperou" → "pedido de parada", ou quando `MaxAbandonedJobs` jobs rodam sem worker (o próximo job travado vai segurar um worker de vez). `/health` (readiness) continua `200` nesse caso.
- **Degraded** quando a fila está parada (algum job de uma fila **consumida por este processo** em `enqueued` há mais que `StalledQueueThreshold`, padrão 60 s) ou algum circuit breaker de job está `open` (`half-open` não degrada: já deixa passar um job de teste), ou algum job roda sem worker há mais que `AbandonedJobAlertAfter`. `/health/details` lista esses jobs em `abandoned` (`count`, `limit`, `full`, `alert`, `jobs[]` com `id`, `name`, `since`, `ageSeconds`).
- **Healthy** caso contrário. O `data` do resultado traz `ready`, `queue.*` e o estado de cada breaker.

```csharp
builder.Services.AddHealthChecks().AddCheck<JobServerHealthCheck>("jobs");
app.MapHealthChecks("/ready");
```

Ou use as rotas de `MapBackgroundJobsApi()`: `/health` (readiness, contrato da spec) e `/health/details`:

```json
{
  "status": "degraded", "ready": true,
  "queue": { "ready": 12, "oldestReadyAgeSeconds": 74.2, "stalled": true, "stalledThresholdSeconds": 60, "sampledAt": "…Z",
             "queues": [{ "name": "default", "consumed": true, "ready": 12, "oldestReadyAgeSeconds": 74.2 }] },
  "circuitBreakers": [ { "job": "send-email", "state": "open", "consecutiveFailures": 5, "openUntil": "…Z" } ]
}
```

A fila é amostrada a cada `QueueMonitorInterval` (padrão 5 s), então idade e profundidade podem estar até um intervalo atrasadas; `queue.ready` é `null` antes da primeira amostra. `ready`/`oldestReadyAgeSeconds`/`stalled` somam só as filas consumidas; `queues[]` detalha cada uma (filas consumidas vazias aparecem com 0, e as demais com jobs prontos com `consumed: false`). `orphaned: true` (e status `degraded`) quando **nenhum nó vivo** consome a fila e o job mais antigo dela espera há mais que `StalledQueueThreshold`, por exemplo um enqueue numa fila com nome errado. Cada nó renova o registro das suas filas a cada `LeaseDuration / 3`. Jobs bloqueados por breaker aberto continuam em `enqueued` e contam para a fila parada. O mesmo vale para jobs de um nome que nenhum nó vivo registrou: a fila fica `stalled` em todo nó que a consome (e `orphaned` se nenhum nó a consome). O health não diz quais nomes estão sem nó; para achá-los, liste os jobs `enqueued` antigos (no dashboard, `GET {prefix}/?state=enqueued`; a API `/api/jobs` não filtra por estado) e compare com os nomes registrados.

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
| `backgroundjobs.queue.depth` | gauge | `{job}` | `queue` — jobs em `enqueued` |
| `backgroundjobs.queue.oldest_age` | gauge | `s` | `queue` — idade do job mais antigo em `enqueued` (0 se vazia) |
| `backgroundjobs.jobs.abandoned` | gauge | `{job}` | — jobs rodando além do timeout/cancelamento sem worker **neste nó** |

Os gauges da fila vêm da mesma amostra do health check e são globais ao banco (todos os nós reportam o mesmo valor). `backgroundjobs.jobs.abandoned` é a exceção: é por processo, então some entre nós.

### Logs estruturados

Todo log emitido durante uma tentativa — inclusive os do próprio job via `ILogger` — tem no scope `JobId`, `JobName`, `Attempt` e `Run`. Habilite scopes no seu provider (ex.: `Logging:Console:FormatterOptions:IncludeScopes=true`).

## 11. Banco de dados

- Migrations embutidas são aplicadas **automaticamente** na partida, numa transação protegida por advisory lock (seguro com vários nós subindo juntos). Controle em `bj_migrations`.
- Tabelas: `bj_jobs`, `bj_attempts`, `bj_history`, `bj_queue_consumers`, `bj_recurring` (+ sequence `bj_enqueue_seq`). O usuário do banco precisa de permissão de `CREATE` no schema na primeira execução.
- Jobs finalizados são removidos automaticamente após `FinishedJobRetention` (veja [Retenção](#retenção)), junto com `bj_attempts`/`bj_history`.

## 12. Operação: múltiplos nós, crash e shutdown

- **Múltiplos nós:** suportado. O `NodeId` padrão é único por instância (nome da máquina + sufixo aleatório), então várias réplicas na mesma máquina ou imagem não colidem. Se definir o seu, mantenha-o único: dois processos vivos com o mesmo id renovariam os leases um do outro.
- **Lease e heartbeat:** cada job em execução tem um lease de `LeaseDuration`, renovado a cada `LeaseDuration / 3`. Se o nó morrer, outro nó recupera o job após o lease expirar; a tentativa vira `interrupted` e conta para `MaxAttempts`. O lease é medido pelo relógio do Postgres (`now()`), tanto ao gravar quanto ao verificar, então diferença de relógio entre nós não faz um nó recuperar o job de outro. Durante a parada graciosa o lease continua sendo renovado até o último job terminar. Um nó que perde o lease de um job (o banco não respondeu por `LeaseDuration`, ou a renovação mostra que o job já foi recuperado) dispara o token do job na hora, para não executá-lo em paralelo com um novo dono e não grava o resultado dessa execução (se ninguém assumiu o job, ele é recuperado quando o lease expira).
- **Reinício após crash:** a recuperação é sempre por lease expirado, nunca pelo id do nó — os jobs do processo morto voltam à fila em até ~`LeaseDuration` (+ `LeaseDuration / 3` do heartbeat). Reduza `LeaseDuration` se precisar de recuperação mais rápida. **Mudança de comportamento:** antes, reiniciar com o mesmo `NodeId` fixo recuperava os jobs na hora; agora também espera o lease. Na parada graciosa o nó deixa de contar como consumidor das suas filas na hora; após um crash, isso só acontece quando o registro expira (~`LeaseDuration`).
- **Parada graciosa:** ao parar, o servidor deixa de pegar novos jobs, sinaliza `context.CancellationToken` e **espera os em execução terminarem**, até `ShutdownTimeout` (sem limite por padrão); jobs que param pelo token, ou que passam do limite, voltam para a fila na hora sem gastar tentativa (o HTTP continua aceitando enqueues durante o drain). Ajuste o timeout do host conforme sua plataforma:

  ```csharp
  builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = Timeout.InfiniteTimeSpan);
  ```

  Em Kubernetes/ECS, alinhe `terminationGracePeriodSeconds` com a duração dos seus jobs mais longos.
- **Partida:** `StartAsync` retorna imediatamente; migração e recuperação são refeitas em background até o banco responder.

## 13. Limitações e boas práticas

- **Entrega at-least-once:** um job pode executar mais de uma vez (crash, ou banco indisponível por mais que `LeaseDuration` durante a execução). **Escreva jobs idempotentes.**
- **Cancelamento cooperativo:** .NET não mata threads; um job que ignora o token continua rodando (sem worker, até `MaxAbandonedJobs`) até voltar, e atrasa o shutdown até `ShutdownTimeout`. Depois de um `ShutdownTimeout`, outro nó pode rodar o job enquanto este processo ainda encerra.
- **Retry fixo**, sem backoff exponencial nativo (use `RetryJitter` para espalhar).
- **Fila e prioridade são fixas** depois do enqueue. A garantia contra starvation é uma fração dos claims (`FifoClaimEvery`), não aging.
- **Circuit breaker de job** é por processo, não compartilhado entre nós.
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
