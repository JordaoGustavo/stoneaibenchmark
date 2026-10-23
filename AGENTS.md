# AGENTS.md

Guia para agentes de IA (e pessoas) que usam a biblioteca BackgroundJobs ou trabalham neste repositório.
Referência completa da API: [docs/API.md](docs/API.md). Conceitos e operação: [docs/INTEGRACAO.md](docs/INTEGRACAO.md).

---

## Parte A: Usando a biblioteca

### Registro mínimo

```csharp
using BackgroundJobs;

builder.Services
    .AddBackgroundJobs(
        o => o.FinishedJobRetention = TimeSpan.FromDays(7),            // OBRIGATÓRIO
        jobs => jobs.Register<SendEmailJob, SendEmailArgs>("send-email"))
    .UsePostgres(connectionString);                                     // ou .UseInMemoryStorage() em testes

public sealed record SendEmailArgs(string To, string Subject);

public sealed class SendEmailJob(IEmailSender sender) : IJob<SendEmailArgs>
{
    public Task ExecuteAsync(SendEmailArgs args, JobContext context) =>
        sender.SendAsync(args.To, args.Subject, context.CancellationToken);
}
```

- Pacotes: `BackgroundJobs.Postgres` (traz o núcleo); opcionais `BackgroundJobs.AspNetCore` (`app.MapBackgroundJobsApi()`)
  e `BackgroundJobs.Dashboard` (`app.MapBackgroundJobsDashboard()`). Hoje via `ProjectReference`.
- O job **não** precisa ser registrado no container de DI: ele é criado com `ActivatorUtilities` num scope novo a
  cada tentativa. Só as dependências dele precisam estar registradas (serviços scoped funcionam).
- Lançar exceção = tentativa falhou. Não capture e engula erros que devem gerar retry.

### Enfileirar e agendar

```csharp
var id = await jobs.EnqueueAsync("send-email", new SendEmailArgs("a@b.com", "Oi"));          // agora
await jobs.EnqueueAsync("send-email", args, new JobOptions { RunAt = DateTimeOffset.UtcNow.AddMinutes(10) }); // agendado
await jobs.EnqueueAsync("send-email", args, new JobOptions
{
    MaxAttempts = 5, RetryDelay = TimeSpan.FromSeconds(30), RetryJitter = TimeSpan.FromSeconds(5),
    Queue = "emails", Priority = 10, IdempotencyKey = $"welcome:{userId}",
});
```

Lote atômico: `jobs.EnqueueManyAsync(items.Select(i => new JobToEnqueue("send-email", args, options)))`. Outbox no Postgres:
`jobs.EnqueueAsync(npgsqlTransaction, "send-email", args)` (ou `EnqueueManyAsync(tx, ...)`): o job só existe se a
transação da aplicação fizer commit.

`IJobClient` (singleton) também tem `GetAsync`, `ListAsync`, `DeleteAsync`, `RequeueAsync`, `CancelAsync`. Não existe
método `Schedule`: agendar é `EnqueueAsync` com `RunAt`.

### Opções que mais importam

| Onde | Opção | Padrão |
|---|---|---|
| `BackgroundJobsOptions` | `FinishedJobRetention` | nenhum: **obrigatória** (`Timeout.InfiniteTimeSpan` = guardar para sempre) |
| | `WorkerCount` / `Queues` | `4` workers na fila `default` / vazio |
| | `DefaultMaxAttempts`, `DefaultRetryDelay`, `DefaultRetryJitter` | `3`, `1 s`, `0` |
| | `DefaultJobTimeout` | infinito |
| | `LeaseDuration` | `30 s` |
| | `ShutdownTimeout` | infinito |
| `JobRegistrationOptions` (no `Register`) | `ArgsVersion` + `UpgradeArgs`, `Timeout`, `MaxConcurrency`, `CircuitBreaker` | `1`, `null`, `null`, `null` |

Lista completa e validações em [docs/API.md](docs/API.md#opções).

### Recorrentes

```csharp
builder.Services.AddBackgroundJobs(o => o.FinishedJobRetention = TimeSpan.FromDays(7),
        jobs => jobs.Register<ReportJob, ReportArgs>("daily-report"))
    .UsePostgres(cs)
    .AddRecurringJob("daily-report", "daily-report", "0 9 * * MON-FRI", new ReportArgs("vendas"),
        new RecurringJobOptions { TimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/Sao_Paulo") });
```

Em tempo de execução: `IRecurringJobClient.UpsertAsync / GetAsync / ListAsync / RemoveAsync / TriggerAsync`. Cron de 5
campos, ou 6 com segundos primeiro. O fuso padrão é UTC.

### Idempotência

- `JobOptions.IdempotencyKey`: enquanto um job com a chave estiver `scheduled`, `enqueued` ou `processing`, um novo
  enqueue com a mesma chave **não cria nada e retorna o id do job existente**, ignorando o nome, os args e as opções
  novos. Quando o job finaliza, a chave fica livre. Máximo de 200 caracteres, não pode ser em branco.
- Isso deduplica **enfileiramentos**, não **execuções**. A entrega é at-least-once: o código do job também precisa ser
  idempotente.

### Cancelamento e timeout

- Repasse `context.CancellationToken` para toda chamada de I/O. Ele dispara em timeout, `CancelAsync`, parada do
  servidor e perda do lease.
- `CancelAsync` num job em `processing` só dispara o token; o job termina em `cancelled` quando o código parar. Se o
  código ignorar o token e concluir com sucesso, o job fica `succeeded`.
- Um job que ignora o token continua em `processing` (sem worker depois de `TimeoutGracePeriod`, até `MaxAbandonedJobs`)
  até voltar. Um job que nunca volta fica em `processing` para sempre.

### Armadilhas (verificadas no código)

1. **`FinishedJobRetention` é obrigatória.** Sem ela, o host não inicia (`ArgumentException` na validação das opções).
2. **É obrigatório escolher um store.** Sem `.UsePostgres(...)` nem `.UseInMemoryStorage()`, a partida falha com
   `InvalidOperationException: no job store configured`.
3. **Args são persistidos** como JSON (`JsonSerializerDefaults.Web`, camelCase) e lidos quando o job roda, talvez depois
   de um deploy. Mudança compatível (propriedade nova **com valor padrão**, remoção): mantenha a versão. Mudança
   incompatível (renomear, trocar tipo): incremente `ArgsVersion` e registre `UpgradeArgs(versãoAnterior, json => ...)`,
   que trabalha com `JsonObject` em camelCase. `Register` exige um upgrade para cada versão de `1..ArgsVersion-1`.
   Um job gravado com versão mais nova que a do código falha a tentativa.
4. **O nome do job é a chave persistida.** Não renomeie um job enquanto houver instâncias pendentes.
5. **Nomes desconhecidos são aceitos no enqueue** e não há validação de nome no `EnqueueAsync`. Um nó só faz claim de
   jobs cujo nome ele registrou, então um job que nenhum nó vivo registrou fica em `enqueued` para sempre (sem gastar
   tentativas; a fila aparece `stalled` no health) até subir um nó com o registro, como num deploy gradual.
   Já `IRecurringJobClient.UpsertAsync` exige job registrado.
6. **Args de tipo diferente do registrado** lançam `ArgumentException` no enqueue (`JsonElement`/`JsonNode` passam como
   JSON cru).
7. **Entrega at-least-once.** Crash, ou banco fora por mais que `LeaseDuration`, pode rodar o job de novo. Escreva jobs
   idempotentes.
8. **Retry é de atraso fixo** (`RetryDelay` + jitter), sem backoff exponencial. `MaxAttempts` conta a primeira
   execução.
9. **Job em `processing` não pode ser deletado** (`InvalidJobTransitionException`): use `CancelAsync`. `DeleteAsync`
   aceita só `scheduled`/`enqueued`; `RequeueAsync`, só estados finalizados.
10. **Fila sem consumidor:** um job numa fila que nenhum nó lista em `Queues` fica em `enqueued` para sempre (o health
    check mostra a fila como órfã). Nome de fila: `^[a-z0-9][a-z0-9_.-]{0,63}$`.
11. **`MaxConcurrency` vale para o cluster inteiro** e precisa ser configurado igual em todos os nós. O circuit breaker
    é por processo.
12. **A biblioteca não lê `IConfiguration`**: faça o bind no delegate. URLs `postgres://` não são aceitas por
    `UsePostgres`, só a connection string Npgsql.
13. **Erros de `AddRecurringJob`** com job não registrado ou opções inválidas não derrubam a partida: são logados e a
    definição não é criada. Remover `AddRecurringJob` do código não apaga a definição (use `RemoveAsync`).
14. **A API HTTP não cria jobs** e não tem autenticação. O dashboard tem uma tela de enqueue (`/jobs/new`) e também não
    tem autenticação: use `.RequireAuthorization(...)`.
15. **`NodeId` precisa ser único** entre os processos vivos. O padrão já é único; não fixe o mesmo valor em réplicas.
16. Datas retornadas são UTC (`DateTime`). `ListAsync` aceita `limit` de 1 a 1000.

---

## Parte B: Trabalhando neste repositório

### Layout

| Caminho | Conteúdo |
|---|---|
| `src/BackgroundJobs` | Núcleo: `Abstractions/` (contratos públicos), `DependencyInjection/`, `Domain/` (estados, `JobTransition`), `Processing/` (servidor, dispatcher, scheduler, heartbeat), `Registry/`, `Storage/` (SPI + store em memória), `Health/`, `Telemetry/` |
| `src/BackgroundJobs.Postgres` | Store Postgres, listener LISTEN/NOTIFY, `Migrations/NNN_*.sql` (embutidas) |
| `src/BackgroundJobs.AspNetCore` | `ManagementEndpoints` (API HTTP) |
| `src/BackgroundJobs.Dashboard` | Dashboard HTML |
| `src/BackgroundJobs.Demo` | App de demonstração usada pelos testes E2E (configurada por variáveis de ambiente, veja `RUNNING.md`) |
| `samples/` | Exemplos mínimos de uso ([samples/README.md](samples/README.md)) |
| `tests/BackgroundJobs.E2E` | Cenários de aceite que sobem a demo como processo real (`Scenarios/Sxx_*.cs`, `Infra/`) |
| `tests/BackgroundJobs.StoreContract` | Os mesmos testes de `IJobStore` contra Postgres e memória |
| `docs/` | `INTEGRACAO.md`, `API.md`, `SPEC.md` (spec original do desafio) |

### Comandos

```sh
dotnet build                                   # warnings são erros
dotnet test                                    # tudo; precisa do Docker (Testcontainers)
dotnet test tests/BackgroundJobs.StoreContract # só o contrato do store (~10 s)
dotnet test tests/BackgroundJobs.E2E --filter "FullyQualifiedName~S19"   # um cenário
docker compose up -d postgres                  # Postgres local para a demo
dotnet run --project src/BackgroundJobs.Demo   # demo em :8080 (painel em /jobs)
docker compose --profile observability up -d   # + Grafana/OTLP
```

SDK fixado em `global.json` (`10.0.402`, `rollForward: latestFeature`). Versões de pacotes centralizadas em
`Directory.Packages.props`.

### Convenções

- **Prefira cenários E2E** a testes unitários: um arquivo novo em `tests/BackgroundJobs.E2E/Scenarios`, numerado em
  sequência (`S24_...`), dirigindo a demo pela HTTP. Se o comportamento precisar de algo novo na demo, exponha via
  variável de ambiente ou endpoint `/demo/*`. Cenários que não dependem de durabilidade rodam contra todos os stores.
- **Mudou `IJobStore` ou o comportamento de um store?** Adicione o caso em `tests/BackgroundJobs.StoreContract`; ele roda
  contra `PostgresJobStore` e `InMemoryJobStore`, e os dois precisam passar.
- **Regras de transição** só em `src/BackgroundJobs/Domain/Transitions.cs` (`JobTransition`). Stores só aplicam.
- **Migrations SQL numeradas**: novo arquivo `src/BackgroundJobs.Postgres/Migrations/NNN_descricao.sql` com o próximo
  número (hoje o último é `010`). São aplicadas em ordem de nome e registradas em `bj_migrations`: **nunca edite uma
  migration existente**. Comente o porquê de cada índice/coluna no próprio SQL.
- **`TreatWarningsAsErrors`** está ligado para todos os projetos (`Directory.Build.props`), assim como `Nullable`.
- **API pública**: os pacotes usam `Microsoft.CodeAnalysis.PublicApiAnalyzers`. Ao adicionar, mudar ou remover um membro
  público, atualize o `PublicAPI.Unshipped.txt` do projeto, senão o build falha. Atualize também
  [docs/API.md](docs/API.md) e, se for o caso, `docs/INTEGRACAO.md` e o [CHANGELOG.md](CHANGELOG.md) (seção `Unreleased`).
- Documentação para humanos em português; identificadores de código como estão. Comentários de código em inglês.
- Não use bibliotecas prontas de processamento de jobs: o motor é próprio por requisito ([docs/SPEC.md](docs/SPEC.md)).
