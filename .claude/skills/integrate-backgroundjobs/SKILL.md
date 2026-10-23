---
name: integrate-backgroundjobs
description: Integrates the BackgroundJobs .NET library (PostgreSQL-backed background jobs) into an application. Use when the user wants to add background jobs, queues, scheduled/delayed/recurring (cron) jobs, retries or a job worker to a .NET app, or asks to integrate, register, enqueue or configure BackgroundJobs, IJobClient, IJob<TArgs>, AddBackgroundJobs or UsePostgres.
---

# Integrar BackgroundJobs

Antes de escrever código, leia `AGENTS.md` (parte A, sobretudo "Armadilhas") e consulte `docs/API.md` para cada
assinatura que for usar. Não invente membros: se algo não está em `docs/API.md`, confira em `src/BackgroundJobs/Abstractions/`.

## Checklist

1. **Entenda o caso de uso.** Liste os jobs (nome estável, args, o que fazem), quais são imediatos, agendados ou
   recorrentes, quais filas e quantos workers, e se a app tem HTTP (API/Dashboard) ou é um worker.
2. **Referências de projeto.** Adicione `ProjectReference` para `src/BackgroundJobs.Postgres/BackgroundJobs.Postgres.csproj`
   (traz o núcleo) e, se quiser, para `BackgroundJobs.AspNetCore` e `BackgroundJobs.Dashboard`. Alvo `net10.0`.
3. **Args.** Um `record` simples e serializável por `System.Text.Json` (camelCase). Propriedades novas no futuro sempre
   com valor padrão.
4. **Job.** `sealed class XJob(deps...) : IJob<XArgs>`. Repasse `context.CancellationToken` a todo I/O. Lance exceção
   para falhar (gera retry). Faça o job **idempotente**, porque a entrega é at-least-once. Não registre o job no DI.
5. **Registro.**
   ```csharp
   builder.Services
       .AddBackgroundJobs(o =>
       {
           o.FinishedJobRetention = TimeSpan.FromDays(7); // obrigatório
           // o.Queues = new() { ["default"] = 4, ["critical"] = 2 };
       }, jobs => jobs.Register<XJob, XArgs>("x", r => { /* r.Timeout, r.MaxConcurrency, r.CircuitBreaker */ }))
       .UsePostgres(builder.Configuration.GetConnectionString("Jobs")!);
   ```
   Connection string no formato Npgsql (`Host=...;Database=...`), não `postgres://`. Faça o bind de `IConfiguration`
   manualmente.
6. **Enfileirar.** Injete `IJobClient` e use `EnqueueAsync("x", new XArgs(...), new JobOptions { ... })`. Use
   `RunAt` para agendar e `IdempotencyKey` quando o mesmo pedido puder chegar duas vezes (por exemplo, um webhook).
   Use o mesmo nome do `Register`: nome errado só falha na execução. Considere uma constante para o nome.
   - **Vários jobs de uma vez:** `EnqueueManyAsync(new[] { new JobToEnqueue(nome, args, opções), ... })` é atômico
     (todos ou nenhum) e retorna os ids na ordem de entrada.
   - **Junto de uma escrita da app (outbox, só Postgres):** `client.EnqueueAsync(tx, nome, args, opções)` ou
     `client.EnqueueManyAsync(tx, jobs)` com a `NpgsqlTransaction` aberta no mesmo banco de `UsePostgres`. Os jobs só
     existem se ela fizer commit; use `READ COMMITTED`. Prefira isso a "gravar e depois enfileirar".
   - **Encadear:** `JobOptions.ContinueAfter = idDoPai` deixa o job em `awaiting` até o pai finalizar.
     `Condition = OnSucceeded` (padrão; senão fica `cancelled`) ou `OnFinished` (roda em qualquer estado final, bom
     para limpeza). Pai inexistente lança `JobNotFoundException`.
   - **Retry:** atraso fixo por padrão. Para falhas transitórias de serviços externos use
     `RetryBackoff = RetryBackoff.Exponential` (+ `RetryBackoffMultiplier`, `MaxRetryDelay` como teto), por job ou via
     `DefaultRetryBackoff`/`DefaultRetryBackoffMultiplier`/`DefaultMaxRetryDelay` em `BackgroundJobsOptions`.
7. **Recorrentes**, se houver: `.AddRecurringJob(id, jobName, cron, args, new RecurringJobOptions { TimeZone = ... })`
   no builder. O job precisa estar registrado; um erro aqui é só logado na partida.
8. **HTTP opcional.** `app.MapBackgroundJobsApi()` e/ou `app.MapBackgroundJobsDashboard().RequireAuthorization(...)`.
   Proteja as duas superfícies: nenhuma tem autenticação.
9. **Health e telemetria.** `AddHealthChecks().AddCheck<JobServerHealthCheck>("jobs")` (namespace
   `BackgroundJobs.Health`); OpenTelemetry com `AddSource`/`AddMeter(BackgroundJobsTelemetry.Name)`.
10. **Operação.** Alinhe `HostOptions.ShutdownTimeout` e `BackgroundJobsOptions.ShutdownTimeout` com o job mais longo. Não fixe o mesmo `NodeId`
    em réplicas. Revise `LeaseDuration` se a recuperação precisar ser mais rápida que cerca de 30 s.
11. **Testes.** Use `.UseInMemoryStorage()` em testes de integração da app (nunca em produção). Verifique o estado
    final com `IJobClient.GetAsync(id)` (`Job.State`, `AttemptHistory`).
12. **Mudanças futuras nos args.** Se forem incompatíveis, incremente `ArgsVersion` e registre `UpgradeArgs`; veja
    "Versionando os args" em `docs/INTEGRACAO.md`.

## Verificação final

- [ ] `FinishedJobRetention` definida e um store escolhido
- [ ] Todo nome usado em `EnqueueAsync` está registrado com o mesmo tipo de args
- [ ] Jobs idempotentes e repassando o `CancellationToken`
- [ ] Cada fila usada em `JobOptions.Queue` é consumida por algum nó (`BackgroundJobsOptions.Queues`)
- [ ] Rotas HTTP/dashboard protegidas
- [ ] Jobs que dependem de uma escrita no banco são enfileirados na mesma transação (outbox), não depois do commit
- [ ] `dotnet build` sem warnings (o repositório usa `TreatWarningsAsErrors`)
