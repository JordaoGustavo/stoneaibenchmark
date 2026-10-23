# Changelog

Todas as mudanças relevantes deste projeto são registradas aqui.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/) e o projeto adota
[Versionamento Semântico](https://semver.org/lang/pt-BR/). Enquanto a versão for `0.x`, a API pública ainda pode mudar
entre versões menores.

## [Unreleased]

### Alterado

- **Mudança de comportamento:** um nó só faz claim de jobs cujo nome está registrado no seu `JobRegistry`. Antes, com
  vários nós no mesmo banco (deploy gradual, ou duas aplicações na mesma fila), um nó pegava jobs que não conhecia e eles
  falhavam com `unknown job '<nome>'`, gastando tentativas até `failed`. Agora esses jobs ficam em `enqueued`, sem gastar
  tentativas, até um nó que os registre fazer o claim; enquanto isso a fila aparece parada (`stalled`) em
  `/health/details`. O erro `unknown job` na execução fica só como rede de segurança.

### Adicionado

- Dashboard: jobs encadeados. Seção "Run after" no formulário de disparo (id do pai validado + condição), botão
  "Add continuation" no detalhe do job (`/new?after={id}`) e construtor de cadeia em `/jobs/chain` (passos A → B → C
  com args e condição, reordenáveis, validados antes de enfileirar qualquer passo). Funciona sem JavaScript.
- Continuações: `JobOptions.ContinueAfter` (id do job pai) e `JobOptions.Condition` (`ContinuationCondition.OnSucceeded`,
  padrão, ou `OnFinished`). O job espera no novo estado `JobState.Awaiting` e, na mesma transação em que o pai finaliza,
  é enfileirado (ou agendado, se o `RunAt` ainda estiver no futuro) ou cancelado (`continuation_condition_not_met`) se a
  condição não foi atendida. `Job.ContinueAfter`/`Job.Condition`, campos `continueAfter`/`continuationCondition` na API
  HTTP e no dashboard. Migration `011_continuations`. SPI: `NewJob.ContinueAfter`/`Condition`, e toda transição para um
  estado finalizado precisa liberar as continuações (veja `IJobStore`).
- Enfileiramento em lote: `IJobClient.EnqueueManyAsync(IEnumerable<JobToEnqueue>)`, atômico, com opções e
  `IdempotencyKey` por job; no Postgres é um único `INSERT` a partir de arrays. Novo método `IJobStore.InsertManyAsync`
  no SPI: stores de terceiros precisam implementá-lo.
- Enfileiramento transacional (outbox) no Postgres: `PostgresJobClientExtensions.EnqueueAsync(tx, ...)` e
  `EnqueueManyAsync(tx, ...)` gravam os jobs na `NpgsqlTransaction` da aplicação; eles só existem se ela fizer commit.
  Lote e transação aceitam todas as opções de `JobOptions`, incluindo continuações (pai e filho podem nascer na mesma
  transação) e backoff de retry.
- `ClaimRequest.Names` (SPI de store): restringe o claim a esses nomes; `null` aceita qualquer nome. Stores de terceiros
  precisam aplicar o filtro (veja "Escrevendo um novo store" em `docs/INTEGRACAO.md`).
- Backoff exponencial de retry, opt-in: `JobOptions.RetryBackoff` (`RetryBackoff.Fixed`/`Exponential`),
  `RetryBackoffMultiplier` e `MaxRetryDelay`, com padrões globais em `BackgroundJobsOptions.DefaultRetryBackoff`,
  `DefaultRetryBackoffMultiplier` (`2`) e `DefaultMaxRetryDelay` (sem teto). O padrão continua sendo atraso fixo. As
  configurações são gravadas com o job (migration `012_retry_backoff.sql`). Jobs de recorrentes usam o padrão global.
  SPI de store: `NewJob`/`ClaimedJob` ganham `RetryBackoffMultiplier` (`1` = fixo) e `MaxRetryDelayMs`, que stores de
  terceiros precisam gravar no insert e devolver no claim.

- Leitura das configurações de retry: `Job.RetryJitter`, `Job.RetryBackoff` (derivado), `Job.RetryBackoffMultiplier` e
  `Job.MaxRetryDelay`; campos `retryJitterMs`, `retryBackoff`, `retryBackoffMultiplier` e `maxRetryDelayMs` no JSON de
  `GET /api/jobs/{id}`; e no detalhe do dashboard. O formulário de enqueue do dashboard (`/jobs/new/{name}`) aceita
  retry jitter, backoff, multiplicador e teto.
- Listagem de continuações: `IJobClient.ListContinuationsAsync(parentId, after, limit)`, `GET /api/jobs/{id}/children`
  e tabela de continuações no detalhe do dashboard. `JobSummary.ContinueAfter` e o campo `continueAfter` na listagem
  HTTP mostram o pai; a lista do dashboard linka o pai. Migration `013_continuations_by_parent.sql` (índice
  `bj_jobs_continuations`). SPI: novo método `IJobStore.ListContinuationsAsync`, que stores de terceiros precisam
  implementar. `IJobClient` ganhou um método: implementações próprias da interface precisam adicioná-lo.

### Corrigido

- Ids de jobs criados no mesmo milissegundo no mesmo processo agora seguem a ordem de criação (UUIDv7 com contador, RFC 9562 §6.2). Antes, `ListAsync` podia devolvê-los fora de ordem.

## [0.1.0] - ainda não publicada

Primeira versão, reconstruída a partir do histórico do git. Nenhum pacote foi publicado ainda.

### Adicionado

- **Núcleo** (`BackgroundJobs`): `AddBackgroundJobs(configure, registerJobs)`, `IJob<TArgs>`, `JobContext`,
  `IJobClient` (`EnqueueAsync`, `GetAsync`, `ListAsync`, `DeleteAsync`, `RequeueAsync`, `CancelAsync`), servidor
  hospedado, retry com atraso fixo (`RetryDelay`) e jitter (`RetryJitter`), histórico de estados e de tentativas.
- **Store PostgreSQL** (`BackgroundJobs.Postgres`, `.UsePostgres(connectionString)`): claim com `FOR UPDATE SKIP LOCKED`,
  lease renovado por heartbeat, versionamento otimista, migrations embutidas aplicadas na partida sob advisory lock.
- **Store em memória** (`.UseInMemoryStorage()`) para testes e execução local.
- **SPI de store** público em `BackgroundJobs.Storage` (`IJobStore`, `IJobStoreListener`, `IJobSignals`); as regras de
  transição ficam centralizadas em `JobTransition`.
- **Claim em lote**: um claim pega até a quantidade de slots livres (`ClaimBatchSize`).
- **LISTEN/NOTIFY** no Postgres (canal `bj_jobs`) para acordar os nós; o scheduler dorme até o próximo `run_at`
  (`SchedulerMaxWait`).
- **Versionamento de args** (migration `002_args_version`): `JobRegistrationOptions.ArgsVersion` e `UpgradeArgs`.
- **Retenção obrigatória** de jobs finalizados (migration `003_retention`): `FinishedJobRetention`,
  `RetentionSweepInterval`, `RetentionBatchSize`.
- **Timeout e cancelamento** (migration `004_cancellation`): `DefaultJobTimeout`, `JobRegistrationOptions.Timeout`,
  `TimeoutGracePeriod`, `MaxAbandonedJobs`, `AbandonedJobAlertAfter`, `ShutdownTimeout`, `IJobClient.CancelAsync`, estado
  `cancelled`.
- **Tentativas devolvidas** na parada (migration `005_refunded_attempts`): `Job.RefundedAttempts`.
- **Filas e prioridades** (migrations `006_queues_and_priorities` e `007_fifo_claims_and_consumers`): `JobOptions.Queue`,
  `JobOptions.Priority`, `BackgroundJobsOptions.Queues`, `FifoClaimEvery`; registro de consumidores por fila e alerta de
  fila órfã no health check.
- **Jobs recorrentes** (migration `008_recurring_jobs`): `IRecurringJobClient`, `BackgroundJobsBuilder.AddRecurringJob`,
  `CronExpression`, `/api/recurring`.
- **Chaves de idempotência** (migration `009_idempotency_keys`): `JobOptions.IdempotencyKey`,
  `IdempotencyKeyConflictException`.
- **Limite de concorrência por tipo** (migration `010_concurrency_limits`): `JobRegistrationOptions.MaxConcurrency`,
  válido no cluster inteiro.
- **Circuit breaker por tipo de job** (`JobRegistrationOptions.CircuitBreaker`) e breaker automático do banco.
- **Observabilidade**: `ActivitySource` e `Meter` `BackgroundJobs` (`BackgroundJobsTelemetry.Name`), scopes de log por
  tentativa, `JobServerHealthCheck` com estados degraded/unhealthy.
- **API HTTP** (`BackgroundJobs.AspNetCore`, `MapBackgroundJobsApi`) e **dashboard** (`BackgroundJobs.Dashboard`,
  `MapBackgroundJobsDashboard`), separados do núcleo.
- `IJobClient.ListAsync` aceita `descending`; o dashboard lista os mais novos primeiro e mostra horários no fuso do
  navegador (cookie `bj_tz`).
- Demo (`src/BackgroundJobs.Demo`), cenários E2E (`tests/BackgroundJobs.E2E`) e suíte de contrato do store
  (`tests/BackgroundJobs.StoreContract`).

### Alterado (mudanças de comportamento durante o desenvolvimento)

- O Postgres saiu do núcleo para `BackgroundJobs.Postgres`; a connection string deixou de ser opção de
  `BackgroundJobsOptions` e passou para `.UsePostgres(...)`. Sem store configurado o host falha na partida.
- `FinishedJobRetention` não tem padrão: sem ela o host não inicia.
- Um job parado pelo shutdown volta para `enqueued` na hora (`interrupted_by_shutdown`) e a tentativa não conta para
  `MaxAttempts`.
- O `NodeId` padrão passou a ser nome da máquina + sufixo aleatório, único por instância.
- A recuperação na partida passou a ser só por lease vencido, nunca pelo `NodeId`: reiniciar um nó com o mesmo id fixo
  não recupera mais seus jobs na hora; eles voltam em até ~`LeaseDuration`.
- Na parada graciosa o nó remove o próprio registro de consumidor das filas, em vez de esperar ele expirar.
- O lease passou a ser gravado e verificado pelo relógio do Postgres (`now()`), e não pelo relógio de cada nó.
- O upsert de um recorrente exige que o nome do job esteja registrado (ao contrário de `EnqueueAsync`).
- `IJobClient.ListAsync` ganhou o parâmetro `descending`, o que quebra implementações próprias de `IJobClient`.

### Corrigido

- Lease: o heartbeat continua até o fim do drain na parada graciosa; jobs cujo lease não foi renovado têm o token
  disparado e seu resultado descartado.
- `ApplyAsync` lê com `FOR UPDATE`, sem 409 falso em delete/requeue concorrentes; o scheduler recua quando a linha
  vencida está travada por outro nó.
- O claim do Postgres usa `state = 'enqueued'` literal, para que o plano genérico use o índice parcial.
- Disparos de recorrentes protegidos pelo `updated_at` da definição; definições que um nó não consegue agendar não
  seguram as demais.

[Unreleased]: https://github.com/JordaoGustavo/stoneaibenchmark/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/JordaoGustavo/stoneaibenchmark/releases/tag/v0.1.0
