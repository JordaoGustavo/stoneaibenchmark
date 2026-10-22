# Biblioteca de jobs em background

## 1. Objetivo

Construir uma biblioteca de jobs em background como projeto comum para um estudo de workflows de desenvolvimento com agentes de IA. O desafio deve permitir observar decisões de especificação, planejamento, implementação e validação, com um resultado verificável.

A aplicação que usa a biblioteca define funções e pede que sejam executadas agora ou a partir de um horário. A biblioteca persiste os pedidos, executa as funções dentro do processo da aplicação e repete tentativas que falham.

Cada participante entrega a biblioteca, uma aplicação demonstrativa e instruções para executar e testar a solução.

## 2. Escopo e liberdade de implementação

São obrigatórios:

- Enfileiramento imediato e agendamento para um instante futuro.
- Uma fila, com quantidade configurável de workers.
- Retry com limite de tentativas e atraso fixo.
- Persistência de jobs, tentativas e histórico.
- Recuperação após encerramento abrupto do processo.
- Consulta, deleção de jobs pendentes e reenfileiramento de jobs finalizados.
- API HTTP de gerenciamento e demo para verificação.

Linguagem, framework, armazenamento, arquitetura e sintaxe da biblioteca são livres. O motor de processamento e agendamento deve ser implementado pelo participante; bibliotecas prontas de processamento de jobs não podem assumir essa responsabilidade. Bibliotecas de HTTP, serialização e acesso a dados são permitidas.

Existe um único processo executor por armazenamento. A solução não precisa coordenar múltiplos servidores.

Ficam fora do escopo: múltiplas filas, prioridades, recorrências, dependências entre jobs, exclusão por chave, hooks públicos, timeout, cancelamento em execução, retenção automática, autenticação e interface gráfica.

## 3. Comportamento da biblioteca

### 3.1 Registro e execução

A aplicação registra funções usando identificadores estáveis e fornece argumentos serializáveis ao criar um job. A biblioteca deve conseguir resolver esses identificadores após um reinício. IDs de jobs são gerados pela biblioteca.

Enfileirar ou agendar devolve o ID somente depois que o pedido estiver persistido. Um job aceito não pode desaparecer silenciosamente.

A função recebe seus argumentos e um contexto com o ID do job e o número da tentativa atual, começando em 1. Erros de uma função não podem interromper o processamento dos demais jobs.

### 3.2 Estados e histórico

| Estado       | Significado                                      |
| ------------ | ------------------------------------------------ |
| `scheduled`  | Aguarda o horário agendado ou o atraso de retry. |
| `enqueued`   | Está pronto e aguarda um worker.                 |
| `processing` | Está sendo executado.                            |
| `succeeded`  | Terminou com sucesso.                            |
| `failed`     | Esgotou as tentativas.                           |
| `deleted`    | Foi removido antes de executar.                  |

Cada mudança de estado deve ser persistida no histórico, com estado, momento e motivo. A criação também gera uma entrada.

Cada tentativa registra número, início, fim, resultado (`succeeded`, `failed` ou `interrupted`) e mensagem de erro, quando houver.

### 3.3 Fila, concorrência e agendamento

- O número de workers é configurável, com padrão 4. Nunca pode haver mais funções de jobs executando simultaneamente que esse limite.
- Um mesmo job não pode executar simultaneamente em dois workers.
- Com um worker, jobs prontos são iniciados em ordem FIFO, pelo momento de entrada na fila. Com vários workers, não há garantia sobre a ordem exata de início ou término.
- Um job agendado não pode começar antes de `runAt`. Horário no passado equivale a enfileirar imediatamente.
- Jobs agendados ou em retry entram na fila quando se tornam elegíveis.
- Em ambiente local sem sobrecarga, um job pronto, sem outros jobs à frente e com worker livre, deve começar em até 2 segundos.

### 3.4 Falhas e retry

Cada job aceita `maxAttempts` (padrão 3, incluindo a primeira execução) e `retryDelayMs` (padrão 1000).

Quando a função falha:

- Se ainda há tentativas disponíveis, o job volta a `scheduled`, elegível a partir do fim da tentativa mais `retryDelayMs`.
- Caso contrário, termina em `failed`.

O atraso é sempre fixo. Uma função que termina sem erro leva o job a `succeeded`.

### 3.5 Persistência e recuperação

Jobs, argumentos, opções, histórico e tentativas devem sobreviver ao reinício, inclusive após `kill -9`.

Antes de iniciar os workers, a biblioteca recupera jobs que ficaram em `processing`: registra a tentativa como `interrupted` e devolve o job a `enqueued`, sem atraso. Se a tentativa interrompida esgotou `maxAttempts`, o job vai para `failed`. A tentativa interrompida conta no limite.

Jobs pendentes continuam disponíveis; agendamentos vencidos ficam elegíveis imediatamente. Jobs finalizados permanecem consultáveis.

A garantia é *at-least-once*, sujeita ao limite de tentativas: uma interrupção pode fazer uma função executar novamente, mesmo que tenha produzido efeitos antes da queda. Não se exige *exactly-once*; a aplicação deve considerar idempotência.

### 3.6 Operações manuais e parada

- **Deletar:** apenas `scheduled` e `enqueued` podem passar a `deleted`. Depois de confirmada a deleção, o job não executa. Nos demais estados, a operação retorna conflito.
- **Reenfileirar:** `succeeded`, `failed` e `deleted` podem voltar a `enqueued`, com o contador de tentativas zerado. Histórico e registros anteriores são preservados; a numeração das novas tentativas recomeça em 1. Nos demais estados, retorna conflito.
- **Parar:** o servidor deixa de buscar novos jobs e aguarda os que estão em execução. Não é obrigatório limitar essa espera. Se o processo for encerrado à força, aplicam-se as regras de recuperação.

## 4. Aplicação demonstrativa e HTTP

A demo importa a biblioteca e define os jobs. Os endpoints de criação apenas validam a entrada e chamam a biblioteca; processamento, agendamento e retry pertencem à biblioteca.

### 4.1 Configuração

| Variável       | Padrão   | Descrição                              |
| -------------- | -------- | -------------------------------------- |
| `PORT`         | `8080`   | Porta HTTP da demo e do gerenciamento. |
| `WORKER_COUNT` | `4`      | Quantidade de workers.                 |
| `DATA_DIR`     | `./data` | Diretório dos dados persistidos.       |

### 4.2 Jobs da demo

| Nome            | Parâmetros                                 | Comportamento                                                                                                                       |
| --------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| `succeed-after` | `delayMs`: inteiro de 0 a 60.000           | Espera o período e termina com sucesso.                                                                                             |
| `fail-n-times`  | `failTimes`: inteiro de 0 a 10             | Falha com mensagem `demo failure` enquanto a tentativa informada pela biblioteca for menor ou igual a `failTimes`; depois tem sucesso. |
| `log-message`   | `message`: string de até 200 caracteres    | Registra a mensagem e termina com sucesso.                                                                                          |

Todos os parâmetros listados são obrigatórios. O contador usado por `fail-n-times` vem da biblioteca, não de estado próprio da demo.

### 4.3 Criação de jobs

`POST /demo/jobs` recebe:

```json
{
  "job": "fail-n-times",
  "params": { "failTimes": 2 },
  "options": {
    "runAt": "2026-10-07T15:00:00.000Z",
    "maxAttempts": 3,
    "retryDelayMs": 1000
  }
}
```

`options` e seus campos são opcionais. Sem `runAt`, o job é enfileirado imediatamente. `maxAttempts` aceita inteiros de 1 a 10; `retryDelayMs`, de 0 a 60.000. A resposta é `202 { "jobId": "..." }`.

### 4.4 Gerenciamento

| Endpoint                     | Resposta de sucesso                                                                                                  |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `GET /api/jobs`              | `200 { "items": [...] }`, com resumo de todos os jobs: `id`, `name`, `state`, `attempts`.                            |
| `GET /api/jobs/{id}`         | `200` com o job completo, conforme abaixo.                                                                           |
| `DELETE /api/jobs/{id}`      | `200 { "id": "...", "state": "deleted" }`.                                                                           |
| `POST /api/jobs/{id}/requeue`| `202 { "jobId": "..." }`.                                                                                            |
| `GET /health`                | `200 { "status": "ok" }` quando o servidor de jobs estiver iniciado e apto a processar. Caso contrário, `503 { "status": "unavailable" }`. |

O job completo contém `id`, `name`, `args`, `state`, `attempts`, `maxAttempts`, `retryDelayMs`, `scheduledAt`, `createdAt`, `updatedAt`, `history` e `attemptHistory`.

- `attempts`: quantidade de tentativas iniciadas desde a criação ou o último reenfileiramento manual.
- `scheduledAt`: instante de elegibilidade enquanto estiver em `scheduled`; nos demais estados, `null`.
- `history`: lista cronológica de `{ "state", "at", "reason" }`.
- `attemptHistory`: lista cronológica de `{ "attempt", "startedAt", "finishedAt", "outcome", "error" }`. Enquanto uma tentativa estiver em execução, `finishedAt` e `outcome` são `null`. `error` é `null` quando não houver erro.

Não são exigidos filtros nem paginação. IDs são strings opacas; datas usam ISO-8601 UTC; argumentos são representados em JSON.

Erros usam `{ "error": "mensagem legível" }`, com `400` para entrada inválida, `404` para ID inexistente e `409` para operação incompatível com o estado atual.

### 4.5 Eventos observáveis

O código dos jobs registra eventos ao entrar e ao terminar. `log-message` também registra um evento com sua mensagem. Esses registros são independentes do histórico da biblioteca e permitem verificar o que realmente executou.

`GET /demo/events` devolve `200 { "items": [...] }`, em ordem de registro. Cada evento contém `type` (`started`, `finished` ou `message`), `jobId`, `attempt`, `at` e `detail`. Em `finished`, `detail` contém `outcome` (`ok` ou `error`); em `message`, contém `message`; em `started`, pode ser `null`.

Os eventos da demo podem ficar em memória e ser perdidos no reinício. Uma execução interrompida pode ter `started` sem `finished`.

## 5. Critérios de aceite

A avaliação usa os mesmos cenários para todas as soluções. Cada cenário parte de um armazenamento vazio, salvo quando exigir reinício ou preservação de histórico.

| #  | Cenário | Resultado esperado |
| -- | ------- | ------------------ |
| 1  | Enfileirar `succeed-after` com espera zero. | Executa e termina em `succeeded`, com histórico e tentativa registrados. |
| 2  | Agendar um job para alguns segundos no futuro; repetir com horário passado. | O primeiro não começa antes do horário; o segundo fica pronto imediatamente. Ambos respeitam a tolerância de início com worker livre. |
| 3  | Enfileirar mensagens A, B e C sequencialmente, com um worker. | Os eventos de mensagem aparecem nessa ordem. |
| 4  | Enfileirar seis jobs de espera com dois workers. | Há sobreposição de execuções, sem ultrapassar duas funções simultâneas nem duplicar a execução de um job. |
| 5  | Executar `fail-n-times` com `failTimes=2`, `maxAttempts=3` e atraso de 500 ms. | Duas falhas e um sucesso, com tentativas 1, 2 e 3 e intervalo mínimo de 500 ms entre o fim de uma tentativa e o início da seguinte. |
| 6  | Executar `fail-n-times` com `failTimes=10` e `maxAttempts=3`, junto de um job que tem sucesso. | O primeiro termina em `failed` após três tentativas; o segundo executa normalmente. |
| 7  | Deletar um job agendado e reenfileirar um job finalizado. | O deletado não executa; o reenfileirado executa com contador reiniciado e histórico anterior preservado. Operações em estados incompatíveis retornam `409`. |
| 8  | Criar jobs agendados e finalizados, encerrar abruptamente e reiniciar usando os mesmos dados. | Estados, argumentos, histórico e tentativas permanecem; os agendados continuam sendo processados. |
| 9  | Encerrar abruptamente durante um job de espera e reiniciar. Repetir com `maxAttempts=1`. | A tentativa interrompida é registrada. Havendo orçamento, ocorre nova tentativa; sem orçamento, o job termina em `failed`. |
| 10 | Solicitar parada com um job em execução e outro aguardando. | O servidor aguarda o primeiro e não inicia o segundo durante a parada. O pendente executa depois do reinício. |

Os tempos são verificados em ambiente local sem sobrecarga. Além dos cenários, a API deve respeitar os contratos e retornar os erros definidos. A avaliação considera comportamento observado, testes apresentados e inspeção do código.

## 6. Entrega e definição de pronto

A entrega inclui:

- Código da biblioteca e da demo.
- Instruções reproduzíveis para instalar, iniciar, testar e reiniciar com os mesmos dados.
- Um exemplo curto de registro e enfileiramento pela API da biblioteca, sem HTTP.
- Testes e evidências dos critérios de aceite, com falhas e limitações explicitadas.
- Registro do workflow: ferramentas e modelos usados, conversas com os agentes, artefatos produzidos, tempo humano aproximado e principais decisões ou retrabalhos. Tokens e custo são registrados quando disponíveis.

A solução está funcionalmente pronta quando atende aos critérios de aceite. Funcionalidades fora do escopo não são necessárias para concluir o projeto.

O estudo considera a correção do resultado, clareza e manutenibilidade do código, esforço humano, tempo total e processo de colaboração com os agentes. Cada pessoa escolhe como planejar, decompor, implementar e validar o trabalho.
