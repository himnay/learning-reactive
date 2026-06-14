# Spring Reactive Service — Movies

Reactive aggregation service built with **Spring WebFlux** and **Java 25**.  
Combines data from the MovieInfo service and the Reviews service into a single `Movie` response.

```
Port: 8082
MovieInfo service: http://localhost:8080
Reviews service:   http://localhost:8081
```

## Architecture

```
Client
  │
  ▼
MoviesController (WebFlux, port 8082)
  ├── MovieInfoRestClient ──► MovieInfo Service (port 8080)
  └── ReviewRestClient    ──► Reviews Service   (port 8081)
```

Key patterns used:
- **Interface-first API** — `MoviesApi` defines the contract, `MoviesController` implements it
- **Fan-out composition** — `flatMap` + parallel `Flux.collectList()` to aggregate responses
- **Retry with backoff** — exponential retry on 5xx errors from upstream services (via `Retry.backoff`)
- **SSE / NDJSON streaming** — `/v1/movies/stream` resubscribes indefinitely with `.repeat()`
- **Global exception handler** — `@RestControllerAdvice` maps domain exceptions to HTTP statuses
- **Java records** for all DTOs — immutable, concise, zero boilerplate

## Prerequisites

| Tool | Version |
|------|---------|
| Java | 25+ |
| Maven | 3.9+ |
| MovieInfo service | running on `8080` |
| Reviews service | running on `8081` |

## Build & Run

```bash
# From this directory:
mvn clean package
java -jar target/spring-reactive-service-movies-0.0.1-SNAPSHOT.jar

# Or from the root multi-module project:
cd ..
mvn clean package -pl spring-reactive-service-movies
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/movies/{id}` | Aggregate movie info + reviews |
| `GET` | `/v1/movies/stream` | NDJSON stream of live MovieInfo events |
| `GET` | `/actuator/health` | Health check |
| `GET` | `/actuator/prometheus` | Prometheus metrics |

### Example

```bash
# Prerequisites: start MovieInfo (8080) and Reviews (8081) services first

# POST a movie info (to the info service)
curl -i -X POST http://localhost:8080/v1/movieinfos \
  -H "Content-Type: application/json" \
  -d '{"movieInfoId":"1","name":"Batman Begins","year":2005,"cast":["Christian Bale"],"release_date":"2005-06-15"}'

# POST a review (to the review service)
curl -i -X POST http://localhost:8081/v1/reviews \
  -H "Content-Type: application/json" \
  -d '{"movieInfoId":1,"comment":"Awesome Movie","rating":9.0}'

# GET aggregated movie
curl -i http://localhost:8082/v1/movies/1

# SSE stream
curl -N -H "Accept: application/x-ndjson" http://localhost:8082/v1/movies/stream
```

## Observability

- **Prometheus**: `GET /actuator/prometheus`
- **Grafana dashboard**: import `src/main/resources/grafana/dashboard.json`
- **Prometheus scrape config**: `src/main/resources/prometheus/prometheus.yml`

```bash
# Quick Prometheus + Grafana via Docker
docker run -d -p 9090:9090 \
  -v $(pwd)/src/main/resources/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus

docker run -d -p 3000:3000 grafana/grafana
```

## Testing

```bash
mvn test
```

The integration test (`MoviesControllerWireMockInt`) uses WireMock (via Spring Cloud Contract) to stub the upstream services, verifying the full aggregation pipeline without running real dependencies.

## Multi-module Maven structure

```
learning-reactive/          ← root POM (learning-reactive-parent)
└── spring-reactive-movies/ ← this module, inherits from root
```

The root `pom.xml` manages shared versions for Spring Cloud and Testcontainers BOMs.
