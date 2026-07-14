# Distributed Rate Limiter

Sistema distribuito di limitazione del traffico (rate limiting) globale, scritto in Go, pensato per
protezione da DoS e controllo delle quote d'uso di API. Il conteggio delle richieste è gestito in modo
decentralizzato tra più nodi, senza un singolo collo di bottiglia centralizzato.

## Architettura

Il sistema è composto da 3 tipi di nodo indipendenti:

- **Edge rate limiter** (stateless, [edge/](edge/)) — interfaccia ad alto throughput che riceve le richieste
  dei client, ne verifica la quota interrogando l'aggregator competente, applica il circuit breaker in caso
  di guasto, e inoltra in modo asincrono i log delle violazioni.
- **Token bucket aggregator** (stateful, [aggregator/](aggregator/)) — mantiene lo stato dei token residui
  per ogni client. Ogni client è assegnato in modo deterministico (hash del `client_id`) a un nodo
  aggregator primario; gli aggregator si scambiano periodicamente lo stato via gossip, sia per la
  tolleranza ai guasti (failover) sia per la replica.
- **Analytics & alerting** (stateless, [analytics/](analytics/)) — riceve in modo asincrono i log delle
  violazioni di quota via NATS ed emette un alert quando un client supera una soglia critica di richieste
  respinte in una finestra temporale.

Comunicazione:
- **edge ↔ aggregator**: gRPC / Protocol Buffers ([proto/ratelimiter.proto](proto/ratelimiter.proto)), sincrona, bassa latenza.
- **aggregator ↔ aggregator**: gRPC, gossip periodico asincrono.
- **edge → analytics**: NATS (publish/subscribe), asincrono, disaccoppiato.

## Pattern architetturali implementati

- **RPC sincrono (gRPC/Protobuf)** — per il controllo istantaneo delle quote tra edge e aggregator.
- **Circuit Breaker** ([edge/circuit_breaker.go](edge/circuit_breaker.go)) — isola i fallimenti verso un
  aggregator (stati Closed/Open/Half-Open) ed eroga un fallback degradato (fail-open) quando tutti i nodi
  sono irraggiungibili, invece di bloccare il traffico legittimo.
- **Asynchronous Event-Driven Messaging** ([edge/violation_publisher.go](edge/violation_publisher.go),
  [analytics/subscriber.go](analytics/subscriber.go)) — le violazioni di quota vengono pubblicate su NATS
  in modo non bloccante e consumate in modo disaccoppiato dal servizio analytics.
- **Sharding + Gossip** — partizionamento orizzontale dello stato tramite hashing del `client_id`, con
  sincronizzazione asincrona via gossip come meccanismo di replica/failover tra i nodi aggregator.

## Struttura del repository

```
edge/          edge rate limiter (routing, circuit breaker, publisher eventi)
aggregator/    token bucket aggregator (stato, gossip, server gRPC)
analytics/     servizio di analytics/alerting (subscriber NATS, soglie, HTTP stats)
internal/
  config/      parametri di configurazione centralizzati
  events/      tipi di evento condivisi (edge -> analytics)
proto/         definizione e codice generato del servizio gRPC
loadtest/      strumento di carico/misura per lo scenario di scalabilità
Dockerfile     build multi-stage parametrica (ARG TARGET=edge|aggregator|analytics)
docker-compose.yml   orchestrazione di nats, aggregator1, aggregator2, edge, analytics
```

## Come si esegue

Prerequisiti: Docker Desktop (per l'esecuzione containerizzata) oppure Go 1.26+ per l'esecuzione locale.

```powershell
docker compose build
docker compose up
```

Una volta che tutti i servizi sono in ascolto, si può mandare una richiesta di prova verso l'edge:

```powershell
curl -H "X-Client-ID: mario" http://localhost:8081/api
```

I contatori/alert correnti di analytics sono ispezionabili su:

```powershell
curl http://localhost:8082/stats
```

## Testing

**Test automatici Go**, unitari e di integrazione (circuit breaker, failover con server gRPC finti):

```powershell
go test ./...
```

**Scenari di scalabilità** ([loadtest/](loadtest/)), da lanciare con il sistema già in esecuzione:

```powershell
go run ./loadtest -scenario=<nome>
```

| Scenario | Cosa misura |
|---|---|
| `concurrency` | correttezza del token bucket sotto accesso concorrente (nessuna race condition) |
| `load` | latenza (p50/p95/p99) al crescere del numero di client concorrenti |
| `failover` | costo in latenza e comportamento del circuit breaker durante un guasto reale di un aggregator |
| `sharding` | equità della distribuzione hash dei client tra i nodi aggregator |
| `gossip-delay` | tempo di propagazione dello stato tra aggregator via gossip |
| `alert-sensitivity` | sensibilità della soglia di alerting a profili di client onesti vs aggressivi |

Ogni scenario scrive i propri risultati in `loadtest/results/*.csv`.

## Configurazione

I parametri principali sono centralizzati in [internal/config/config.go](internal/config/config.go):
capacità e refill rate del token bucket, soglia/cooldown del circuit breaker, soglia/finestra di alerting.
Gli indirizzi di rete (`AGGREGATOR_ADDRESSES`, `NATS_URL`, `PEER_ADDRESSES`, `SELF_ADDRESS`) sono invece
letti da variabili d'ambiente con fallback a valori di default per l'esecuzione locale — è così che
`docker-compose.yml` collega i container tra loro senza indirizzi hardcoded.
