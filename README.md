# Distributed Rate Limiter

Sistema distribuito di rate limiting scritto in Go, con tre nodi indipendenti (edge, aggregator,
analytics) che comunicano in modo sincrono (gRPC) e asincrono (NATS).

## Funzionamento

Il funzionamento dell'applicazione prevede la corretta installazione di Docker.

## Configurazione

### Avvio dei container (Docker Compose)

Per avviare l'applicazione basta eseguire, dalla cartella del progetto:

```
docker compose build
docker compose up -d
```

oppure in un unico comando:

```
docker compose up --build -d
```

Per fermare i container:

```
docker compose down
```

Una volta avviati, l'applicazione è raggiungibile da:

```
curl -H "X-Client-ID: mario" http://localhost:8081/api
```

Le tracce distribuite sono consultabili su `http://localhost:16686`.

### Avvio dei pod (Kubernetes, opzionale)

Per usare Kubernetes serve un cluster locale attivo e il comando `kubectl` configurato. Le immagini
vanno prima pubblicate su un registry locale, perché il cluster Kubernetes usa uno store immagini
separato da quello di Docker:

```
docker compose build
docker run -d -p 5000:5000 --restart=always --name registry registry:2
```

poi, per ciascuna immagine (aggregator1, aggregator2, aggregator3, edge, analytics):

```
docker tag distributed-rate-limiter-edge:latest localhost:5000/distributed-rate-limiter-edge:latest
docker push localhost:5000/distributed-rate-limiter-edge:latest
```

fatto ciò, si avviano i pod con:

```
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/
```

Per fermare tutto:

```
kubectl delete namespace rate-limiter
```

Se il codice viene modificato, vanno rifatti build, tag e push delle immagini, e poi:

```
kubectl rollout restart deployment -n rate-limiter aggregator1 aggregator2 aggregator3 edge analytics
```

### Deployment su Amazon EC2

Bisogna creare un'istanza EC2 (AMI Ubuntu, si consiglia almeno t3.micro/t3.small) con il Security
Group che apre la porta 22 per il traffico SSH e la porta 8081 per l'edge, la chiave `.pem` per la connessione SSH, 
e l'indirizzo IP pubblico dell'istanza (usato sia per connettersi sia per raggiungere l'edge dall'esterno).
Ci si connette con:

```
ssh -i <chiave>.pem ubuntu@<ip-pubblico>
```

si installano Docker e git:

```
sudo apt update
sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER
```

(bisogna disconnettersi e riconnettersi perché il gruppo docker sia effettivo). Si clona il repository:

```
git clone https://github.com/Berna1998/Distributed-Rate-Limiter.git
cd Distributed-Rate-Limiter
```

e si avvia tutto con:

```
docker compose build
docker compose up -d
```

Dal proprio PC l'applicazione è raggiungibile su:

```
curl -H "X-Client-ID: mario" http://<ip-pubblico-ec2>:8081/api
```

Per consultare le tracce distribuite, essendo Jaeger non esposto pubblicamente, si apre un tunnel SSH
dedicato e poi si va su `http://localhost:16686` dal browser:

```
ssh -i <chiave>.pem ubuntu@<ip> -L 16686:localhost:16686
```

## Per poter mandare le richieste all'edge mentre il sistema è in esecuzione:

  ```
  curl -H "X-Client-ID: mario" http://localhost:8081/api        # Linux/macOS
  curl.exe -H "X-Client-ID: mario" http://localhost:8081/api    # Windows (PowerShell)
  ```

- Dove **`X-Client-ID`** è un identificativo del client.
- Identificativi diversi hanno bucket di quota separati.
