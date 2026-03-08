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

> O compose inclui um serviço `kafka-init` que cria os tópicos `order-created`, `order-created-retry` e `order-created-dlt`, pois o Kafka está com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.

## Executar aplicação

```bash
./gradlew bootRun
```

Por padrão, a aplicação sobe na porta `8081` para não conflitar com o Kafka UI.

## Publicação de mensagens

A aplicação expõe o endpoint `POST /orders`, que recebe um `OrderCreatedEvent` e publica o payload no tópico Kafka `order-created` usando um producer do Spring Cloud Stream.

Payload esperado:

- `id` (long)
- `value` (integer)
- `status` (string)

Exemplo de requisição:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "value": 250,
    "status": "CREATED"
  }'
```

Retornos:

- `202 Accepted`: mensagem enviada com sucesso.
- `500 Internal Server Error`: falha ao publicar no broker.

## Consumo com retry não blocante

O projeto possui dois consumidores com Spring Cloud Stream:

- `orderCreatedConsumer`: consome mensagens do tópico `order-created`.
- `orderCreatedRetryConsumer`: consome mensagens do tópico `order-created-retry`.

Fluxo de retry configurado:

1. Falha no consumo inicial (`order-created`) envia para `order-created-retry` com header `x-retry-attempt=1`.
2. O consumidor de retry agenda novo processamento após **30 segundos** (sem bloquear a thread de consumo).
3. Em nova falha, a mensagem é reenviada para `order-created-retry` até completar **3 tentativas**.
4. Ao exceder o limite, a mensagem é enviada para `order-created-dlt`.

Para simular falha, envie eventos com `status=ERROR`.

## Configuração

As propriedades estão em `src/main/resources/application.yml`.

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `SERVER_PORT` (default: `8081`)
