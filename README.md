# scs-retry-demo

Projeto base com Java 21, Spring Boot 4.0.3, Spring Cloud Stream com binder Kafka, suporte ao Spring Web e Lombok.

## Subir ambiente local com Docker Compose

```bash
docker compose up -d
```

Serviços disponíveis:

- Kafka: `localhost:9092`
- ZooKeeper: `localhost:2181`
- Kafka UI: `http://localhost:8080`

> O compose inclui um serviço `kafka-init` que cria o tópico `order-created`, pois o Kafka está com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.

## Executar aplicação

```bash
./gradlew bootRun
```

Por padrão, a aplicação sobe na porta `8081` para não conflitar com o Kafka UI.

## Consumo de mensagens

A aplicação consome mensagens do tópico Kafka `order-created` no formato JSON com os campos:

- `id` (long)
- `value` (integer)
- `status` (string)

A mensagem é desserializada para `OrderCreatedEvent` e os dados são registrados em log.

Exemplo de payload:

```json
{
  "id": 1,
  "value": 250,
  "status": "CREATED"
}
```

## Configuração

As propriedades estão em `src/main/resources/application.yml`.

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `SERVER_PORT` (default: `8081`)
