# BackgroundJobs

Biblioteca .NET 10 de jobs em segundo plano persistidos em PostgreSQL. A aplicação registra jobs com um nome
estável, pede que rodem agora ou num instante futuro, e a biblioteca grava o pedido, executa o job dentro do próprio
processo e repete tentativas que falham. Vários nós podem compartilhar o mesmo banco: os jobs são pegos com
`FOR UPDATE SKIP LOCKED` e protegidos por lease, e um job que estava rodando num processo que caiu é recuperado.
Não há dependência de motor de jobs de terceiros. O motor de agendamento e processamento é próprio.

## Recursos

- Enfileiramento imediato e agendamento (`JobOptions.RunAt`); jobs recorrentes por cron, com fuso horário
- Retry com limite de tentativas, atraso fixo ou backoff exponencial opcional (com teto) e jitter
- Continuações: um job espera em `awaiting` até o pai finalizar (`ContinueAfter`, condição `OnSucceeded`/`OnFinished`)
- Enfileiramento em lote atômico (`EnqueueManyAsync`) e, no Postgres, dentro da transação da aplicação (outbox)
- Filas nomeadas com workers próprios, prioridades (`-100..100`) e uma fração FIFO contra starvation
- Limite de concorrência por tipo de job no cluster inteiro (`MaxConcurrency`) e circuit breaker por tipo
- Chaves de idempotência no enqueue
- Timeout por tentativa e cancelamento cooperativo (`JobContext.CancellationToken`), inclusive de jobs em execução
- Recuperação após crash por lease vencido; parada graciosa que devolve a tentativa
- Versionamento dos args persistidos (`ArgsVersion` + `UpgradeArgs`)
- Retenção obrigatória de jobs finalizados, com limpeza periódica
- Health check, spans e métricas OpenTelemetry e scopes de log por tentativa
- API HTTP de gerenciamento e dashboard HTML, ambos opcionais; store em memória para testes

## Pacotes

| Pacote | Conteúdo |
|---|---|
| `BackgroundJobs` | Núcleo: `AddBackgroundJobs`, `IJobClient`, `IRecurringJobClient`, `JobRegistry`, store em memória, health check, SPI de store |
| `BackgroundJobs.Postgres` | `.UsePostgres(connectionString)`: store, migrations automáticas e LISTEN/NOTIFY |
| `BackgroundJobs.AspNetCore` | `app.MapBackgroundJobsApi()`: `/api/jobs`, `/api/recurring`, `/health`, `/health/details` |
| `BackgroundJobs.Dashboard` | `app.MapBackgroundJobsDashboard()`: UI HTML renderizada no servidor em `/jobs` |

## Instalação

Por enquanto a biblioteca é consumida via `ProjectReference`:

```xml
<ProjectReference Include="path/to/src/BackgroundJobs.Postgres/BackgroundJobs.Postgres.csproj" /> <!-- traz o núcleo -->
<ProjectReference Include="path/to/src/BackgroundJobs.AspNetCore/BackgroundJobs.AspNetCore.csproj" /> <!-- opcional -->
<ProjectReference Include="path/to/src/BackgroundJobs.Dashboard/BackgroundJobs.Dashboard.csproj" />   <!-- opcional -->
```

Quando os pacotes forem publicados no NuGet, a instalação será `dotnet add package BackgroundJobs.Postgres` (e os
opcionais).

## Início rápido

```csharp
using BackgroundJobs;

var builder = WebApplication.CreateBuilder(args);
builder.Services
    .AddBackgroundJobs(o => o.FinishedJobRetention = TimeSpan.FromDays(7), // obrigatório
        jobs => jobs.Register<GreetJob, GreetArgs>("greet"))
    .UsePostgres("Host=localhost;Username=postgres;Password=postgres;Database=backgroundjobs");

var app = builder.Build();
app.MapBackgroundJobsApi(); // opcional
app.MapPost("/greet/{who}", async (string who, IJobClient jobs) =>
    Results.Accepted(value: new { id = await jobs.EnqueueAsync("greet", new GreetArgs(who)) }));
app.Run();

public sealed record GreetArgs(string Who);
public sealed class GreetJob(ILogger<GreetJob> log) : IJob<GreetArgs>
{
    public Task ExecuteAsync(GreetArgs args, JobContext ctx)
    { log.LogInformation("olá {Who}", args.Who); return Task.CompletedTask; }
}
```

As migrations rodam sozinhas na partida. Para subir um Postgres local: `docker compose up -d postgres`.

## Documentação

- [Guia de integração](docs/INTEGRACAO.md): conceitos, configuração, ciclo de vida, operação
- [Referência da API](docs/API.md): todos os tipos públicos e rotas HTTP
- [Exemplos](samples/README.md): worker sem HTTP, API ASP.NET, recorrentes, idempotência, cancelamento, concorrência,
  backoff exponencial, continuações, outbox transacional e lote
- [Como executar](RUNNING.md): demo, telemetria, testes
- [AGENTS.md](AGENTS.md): guia para agentes de IA (uso da biblioteca e trabalho neste repositório)
- [Especificação original](docs/SPEC.md): o desafio que originou o projeto
- [CHANGELOG](CHANGELOG.md)
