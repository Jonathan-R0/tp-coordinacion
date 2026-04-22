# Informe TP Coordinación Rosenblatt (104105)

El trabajo práctico se realizó sobre el módulo de Python.

## Protocolo interno (colas RabbitMQ)

Los cuerpos de mensaje son listas JSON. Veamos la comunicación de a tramos entre los tipos de nodos:

### Gateway hacia Sum

| Contenido | Significado |
|-------------------------|-------------|
| `[token, fruta, cantidad]` | Mensaje de un cliente. `token` es un UUID único por cliente. |
| `[token]` | Representa el EOF del cliente. |

El `MessageHandler` genera el `token` para cada cliente, lo incluye en los mensajes que envía a Sum y lo usa para identificar el resultado final que le llega de Join. Esto permite asociar cada cliente a su batch de frutas y resultados.

### Sum hacia Aggregator

| Mensaje | Significado |
|---------|-------------|
| `[token, fruta, cantidad]` | Aporte de Sum al shard que corresponde a esa fruta. |
| `[token]` | EOF del Sum: cada Sum envía una por cada Aggregator al cerrar ese cliente. |

### Aggregator hacia Join

| Mensaje | Significado |
|---------|-------------|
| `[token, lista_parcial]` | `lista_parcial` es una lista de pares `[fruta, cantidad]` con el top parcial (hasta `TOP_SIZE` frutas) para ese cliente. |

### Join hacia Gateway

| Mensaje | Significado |
|---------|-------------|
| `[token, lista_final]` | Top global ya fusionado para ese cliente. |

En el gateway, el `deserialize_result_message` del `MessageHandler` del cliente compara el primer campo con su propio `token` y si coincide devuelve `lista_final` para enviarla por TCP como top al cliente. Así varios clientes concurrentes no se mezclan aunque compartan colas internas.

## Enrutamiento de frutas a Aggregators

No se replica cada par fruta–cantidad a todos los Aggregators. La fruta se envía a un nodo específico según un hash estable de su nombre:

`shard = CRC32(nombre_fruta) mod% AGGREGATION_AMOUNT`

(con `zlib.crc32` para que sea deterministico el output).

Solo el exchange `AGGREGATION_PREFIX` con routing key `AGGREGATION_PREFIX_<shard>` recibe ese mensaje de datos.

## Varias réplicas de Sum

### Cola compartida

Todas las Sum consumen la misma working queue desde el gateway y cada mensaje lo procesa un proceso distinto.

Solo una réplica recibe el EOF `[token]` que envía el gateway para un cliente dado.

### Coordinación EOF con fanout

Para que todas las Sum envien su estado parcial para ese `token`, el nodo Sum que recibe el EOF del gateway publica el mismo `token` en un fanout llamado `{SUM_PREFIX}_eof_fanout`. Cada Sum tiene una cola propia enlazada a ese fanout y recibe una copia del mensaje.

Así todas aplican `_flush_client(token)`: envían sus filas acumuladas para ese cliente hacia los nodos correctos y, además, envían el `[token]` a cada routing key de Aggregator.

### Condición de carrera EOF vs último dato

Si el fanout se procesara en un hilo distinto al de la cola de entrada sin más cuidado, podría ejecutarse el flush antes de aplicar un mensaje de datos ya entregado por Rabbit pero aún no mergeado en memoria.

- Con `SUM_AMOUNT > 1` la cola de entrada de Sum usa `prefetch_count = 1`: a lo sumo un mensaje sin ack, así no queda otro registro “en vuelo” por prefetch.
- Los mensajes del fanout se encolan en memoria y el procesamiento del flush se programa con `add_callback_threadsafe` sobre la misma conexión bloqueante que consume la cola de entrada, de modo que el enviado de esos EOF se ejecuta en el mismo hilo que los callbacks de la working queue, en el orden que impone Pika entre callback y callbacks diferidos.
- Tras procesar cada mensaje de la cola de entrada se vuelve a vaciar la cola de coordinación antes del `ack`, de forma que el EOF quede aplicado después del último dato ya procesado en ese consumidor.

### Una sola Sum

Si `SUM_AMOUNT == 1`, no hace falta fanout ni cola de coordinación: el EOF llega por la misma cola que los datos y se hace flush directo.

## Aggregator barrera entre Sum

Cada Aggregator mantiene, por `token`, el acumulado de frutas que le llegan por su shard.

Los mensajes `[token]` incrementan un contador por `token`. Cuando llegaron `SUM_AMOUNT` EOF (uno por cada réplica Sum), se considera que todas las Sum ya enviaron sus aportes y los EOF de barrera para ese shard. Recién ahí se calcula el top parcial y se envía `[token, lista_parcial]` a Join.

## Join fusionado y resultado único por cliente

Join agrupa mensajes por `token`. Necesita recibir exactamente `AGGREGATION_AMOUNT` parciales distintos (uno por cada Aggregator) antes de fusionar: suma cantidades por fruta con `FruitItem`, ordena según la comparación del ítem, toma los `TOP_SIZE` y envía `[token, lista_final]` al gateway.

La cola hacia el gateway es consumida por un solo proceso que entrega el resultado al handler del cliente si el token recibido coincide.

## Middleware RabbitMQ

Además de utilizar las clases ya existentes de la última entrega del tp agregamos una para representar el fanout. Esta se usa en la propagación de mensajes EOF entre los nodos Sum.

## Señal SIGTERM/SIGINT

Sum, Aggregator y Join escuchan las señales `SIGTERM`/`SIGINT` las cuales llaman a `stop_consuming()` y cierran conexiones en un `finally` para terminar los procesos.

## Escalado

La cantidad de nodos del sistema escalan dependiendo de las variables de entorno `SUM_AMOUNT` y `AGGREGATION_AMOUNT`. El Sum siempre tiene en cuenta que pueden haber otros nodos vecinos como para redistribuir los EOFs. Además estos nodos Sum procesan de forma única los mensajes que van recibiendo por una working queue.

La partición por hash envía cada fruta a un Aggregator concreto y luego cada uno espera `SUM_AMOUNT` EOFs del Sum antes de enviar el top parcial al Join.

El Join a su vez junta `AGGREGATION_AMOUNT` tops parciales de un cliente para finalmente devolver la respuesta final.

El sistema escala con la cantidad de nodos ingresados dado que, al paralelizar el trabajo entre procesos, se incrementa la velocidad de procesamiento total ya que hay más nodos para manejar concurrentemente la misma carga de datos.
