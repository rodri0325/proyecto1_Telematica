# Especificación SMDP/1.0 — implementación de la Fase 3

**Alcance:** esta especificación concreta las decisiones de «Fase 1 Diseño y arquitectura» y las reglas que se implementaron. Debe leerse junto con el PDF original. El protocolo original del curso no prescribe nombres de mensajes ni formato; SMDP/1.0 es la decisión de nuestro grupo.

## 1. Propósito, entidades y servicio

Sistema distribuido para supervisar al menos dos nodos. Los **nodos** envían métricas y eventos al **servidor central**; los **clientes administrativos** se autentican y consultan el estado únicamente a través de este servidor. El **servicio de identidad** es un proceso separado que devuelve un perfil de usuario. No hay comunicación directa nodo-cliente.

Primitivas de SMDP/1.0:

| Primitiva | Emisor | Transporte | Respuesta |
|---|---|---|---|
| `REG` | Nodo | TCP | `REG_OK` con token o `ERROR` |
| `STATUS` | Nodo | UDP | Sin ACK, salvo `ERROR` si procede |
| `EVENT` | Nodo | UDP | `ACK` con misma secuencia o `ERROR` |
| `AUTH` | Cliente administrativo | TCP | `AUTH_OK` con perfil y token, o `AUTH_ERR` |
| `QUERY` | Cliente administrativo | TCP | `QUERY_RESP` o `ERROR` |

El servidor principal se implementa en C con sockets Berkeley. Un hilo receptor UDP introduce datagramas en colas FIFO según identificador de nodo. Tres trabajadores UDP procesan esas colas; cada nodo conserva el orden de llegada dentro del servidor. Un hilo independiente atiende cada conexión TCP; los arreglos compartidos y los logs se sincronizan mediante mutex.

## 2. Formato

```
SMDP/1.0|TIPO|ORIGEN|SEQ|TIMESTAMP|TOKEN|PAYLOAD_JSON\n
```

- Codificación UTF-8, terminador LF y **máximo 4096 bytes por mensaje, incluido LF**.
- Los seis primeros separadores `|` delimitan el encabezado; el resto corresponde al JSON compacto. Las solicitudes actuales utilizan un **objeto JSON plano** de máximo 16 propiedades, con valores numéricos o cadenas. Las respuestas pueden incluir arreglos de objetos.
- `TIPO`: operación o respuesta reconocida; `ORIGEN`: identificador del nodo, usuario o `server` para respuestas.
- `SEQ`: entero decimal no negativo y creciente por emisor durante su registro o sesión; respuesta y solicitud comparten secuencia. El nodo comienza nuevamente al volver a registrarse.
- `TIMESTAMP`: época Unix UTC en segundos. Para STATUS y EVENT se conserva el tiempo declarado en el nodo.
- `TOKEN`: guion `-` para REG y AUTH, token de 32 dígitos hexadecimales entregado por `REG_OK` / `AUTH_OK` en el resto de solicitudes. En `ACK` el servidor devuelve el token del nodo para que el nodo verifique la confirmación; en `QUERY_RESP` y `ERROR` utiliza `-`.
- Los identificadores que aparecen en JSON o encabezados y se emplean para búsquedas se limitan a letras y números ASCII, guion, punto y guion bajo; los nodos de ejemplo son `nodo-01` y `nodo-02`.
- **TCP:** cada mensaje ocupa una línea LF. El receptor debe acumular lecturas parciales y procesar múltiples líneas de un mismo `recv`. **UDP:** exactamente un mensaje completo por datagrama (una línea LF).

Ejemplos (el timestamp y los tokens cambian en cada ejecución):

```
SMDP/1.0|REG|nodo-01|1|1788912000|-|{"device_type":"sensor","location":"sala1"}
SMDP/1.0|STATUS|nodo-01|2|1788912010|TOKEN_NODO|{"cpu":32,"temp":41.5,"battery":87,"state":"ok"}
SMDP/1.0|EVENT|nodo-01|3|1788912020|TOKEN_NODO|{"event":"threshold","value":95,"threshold":90}
SMDP/1.0|AUTH|juan|1|1788912050|-|{"user":"juan","pass":"CLAVE_DE_LABORATORIO"}
SMDP/1.0|QUERY|juan|2|1788912060|TOKEN_SESION|{"node":"nodo-01","resource":"history","limit":5}
```

Los `TOKEN_*` son marcadores ilustrativos, no tokens válidos. Cada línea real termina en LF.

## 3. PAYLOAD_JSON y validaciones

| Solicitud | Campos obligatorios y valores |
|---|---|
| `REG` | `device_type`, `location`: identificadores válidos de hasta 63 caracteres. |
| `STATUS` | `cpu` entero de 0 a 100, `temp` número entre -100 y 250, `battery` entero de 0 a 100, `state` identificador de hasta 23 caracteres. |
| `EVENT` | `event` identificador hasta 63 caracteres; `value` y `threshold` números finitos entre -1e9 y 1e9. |
| `AUTH` | `user` y `pass` cadenas simples de hasta 63 caracteres; `user` debe coincidir con ORIGEN. No registrar las contraseñas. |
| `QUERY` | `resource`: `status`, `history`, `events`, `nodes` o `diagnostic`. Para los tres primeros también `node`. `limit` opcional entre 1 y 10 (por defecto 5). Para listado de nodos se admite `offset` (por defecto 0) con paginación. |

### Respuestas

- `REG_OK`: token en encabezado y `{"status":"ok"}`.
- `AUTH_OK`: token de sesión en encabezado y `{"profile":"ADMIN"}` o `{"profile":"VISOR"}`.
- `AUTH_ERR`: `{"code":"UNAUTHORIZED"}`.
- `QUERY_RESP`: JSON que contiene `node` y sus métricas más recientes, `history` (arreglo), `events` (arreglo) o `nodes` (arreglo con `offset`, `shown`, `total`).
- `ACK`: misma secuencia del evento y el token del nodo, con `{"status":"ok"}` para primera confirmación o `{"duplicate":true}` si ya se procesó.
- `ERROR`: `{"code":"..."}` para situaciones recuperables.

Códigos implementados: `MALFORMED_MESSAGE`, `UNSUPPORTED_VERSION`, `UNKNOWN_TYPE`, `NODE_NOT_REGISTERED`, `UNAUTHORIZED`, `NOT_FOUND` e `INTERNAL_ERROR`.

## 4. Procedimientos y estados

### Nodo

1. `INICIO → RESUELTO`: obtener direcciones del dominio del servidor con `getaddrinfo`, con retroceso 1, 2, 4, 8 y máximo 30 segundos si falla.
2. `RESUELTO → REGISTRADO`: establecer TCP y enviar `REG`. Conservar token del `REG_OK`.
3. `REGISTRADO → ACTIVO`: generar y enviar `STATUS` por UDP periódicamente sin esperar `ACK`.
4. `ACTIVO → ESPERA_ACK`: guardar primero el contenido del EVENT pendiente en un archivo local y enviarlo por UDP.
5. Si llega `ACK` del mismo servidor, token y secuencia, retirar el EVENT de la cola pendiente. Si no llega, esperar un segundo y retransmitir **hasta tres veces adicionales**. Si el evento sigue sin confirmación, conservarlo para un intento en otra ejecución.
6. Si el servidor informa `NODE_NOT_REGISTERED`, volver a registrar el nodo y reintentar el evento.

### Servidor

1. `TCP: ESCUCHA → HILO`: aceptar cada conexión TCP en un hilo propio; reconstruir mensajes separados por LF; no cerrar todo el servicio si falla una conexión.
2. `REG`: registrar o re-registrar el nodo, generar un nuevo token y enviar `REG_OK`.
3. `UDP: ESCUCHA → COLA → TRABAJADOR`: identificar el nodo mediante token y ORIGEN, validar los campos y manejar su secuencia.
4. `STATUS`: almacenar únicamente la secuencia de STATUS más reciente y conservar las últimas cinco muestras aceptadas. No confirmar estados válidos.
5. `EVENT`: comprobar secuencias recientes, almacenar el evento nuevo **antes** de confirmarlo y devolver `ACK` también a eventos repetidos.
6. `AUTH`: consultar mediante `getaddrinfo` y TCP el servicio de identidad independiente, generar token y perfil si las credenciales son aceptadas.
7. `QUERY`: comprobar token no vencido, recurso y perfil, consultar bajo mutex y devolver los datos o un error.

### Cliente administrativo

`INICIO → CONECTADO → AUTENTICADO → ESPERA_RESPUESTA → AUTENTICADO`. Si cae la conexión TCP, re-resolver y volver a autenticarse; no reutilizar ciegamente la sesión anterior. `VISOR` accede a `status`, `history` y `events`. `ADMIN` añade `nodes` y `diagnostic`.

### Servicio de identidad (`IDENT/1`, uso interno de laboratorio)

Es un **proceso diferente del servidor principal**, escrito en C, en el puerto configurable del laboratorio, escuchando solo por interfaz local. Intercambia una línea `IDENT/1|usuario|clave\n` por TCP y devuelve `OK|VISOR\n`, `OK|ADMIN\n` o `DENIED\n`. No utiliza sockets distintos de la API Berkeley. Las claves de ejemplo no son credenciales de producción.

## 5. Estados fallidos y política de recuperación

| Situación | Acción |
|---|---|
| Error DNS del cliente | Registro en consola y reintento con espera creciente; el proceso no termina. |
| Error DNS del servicio de identidad | Hasta tres intentos de resolución por autenticación; si falla, se rechaza solo esa autenticación sin detener el servidor. |
| Conexión TCP cerrada o lectura parcial | TCP reconstruye hasta LF; cierre libera el hilo. El cliente puede reconectar. |
| Mensaje grande, JSON erróneo, versión desconocida, parámetros inválidos | `ERROR`; se conserva el proceso y los demás clientes. |
| STATUS UDP fuera de orden | Ignorar la secuencia anterior; aceptar una muestra futura. |
| EVENT UDP duplicado | Responder `ACK` y conservar una única copia dentro de la ventana de deduplicación. |
| ACK perdido | Retener EVENT sin confirmar; retransmitir tras un segundo. |
| Token de nodo inválido | `ERROR NODE_NOT_REGISTERED`. |
| Sesión inválida o vencida | `ERROR UNAUTHORIZED` (vencimiento a los 30 minutos en esta versión). |
| Cola UDP saturada | Descartar datagrama con log; el nodo reintenta EVENT al no recibir ACK. |
| Solicitud incompleta al servicio de identidad | Esperar hasta dos segundos, responder `DENIED` y cerrar esa conexión. |
| Archivo de logs inaccesible | Advertencia en consola; servicio continúa. |

## 6. Log y concurrencia

Formato de una línea (valores de ejemplo):

```
2026-09-29T20:00:00Z transport=UDP peer=IP_DEL_NODO:54321 id=nodo-01 direction=in type=EVENT seq=5 result=RECEIVED
```

Cada solicitud y respuesta se registra con fecha, transporte, IP y puerto del otro extremo, identidad declarada, secuencia y resultado. Por diseño, no se escriben contraseñas, payload completo ni tokens. El archivo y la consola se sincronizan con mutex. La UDP receiver distribuye por hash del identificador de nodo a tres colas FIFO, lo que permite concurrencia entre nodos sin introducir reordenamiento dentro del servidor para un mismo nodo. Los hilos TCP y UDP comparten estado protegido con `pthread_mutex`.

## 7. Limitaciones y no objetivos de esta entrega

1. **Memoria volátil:** historial (5 muestras) y eventos (32 por nodo) permanecen en RAM. La deduplicación abarca las **64 secuencias recientes** por nodo. Una caída con reinicio del servidor no conserva ese historial/eventos; la persistencia duradera e identificadores de evento globales son trabajo futuro. Por ello, esta entrega no promete entrega exactamente una vez ante reinicios.
2. **Laboratorio:** no implementa TLS, cifrado de UDP, autenticación criptográfica de paquetes ni gestión de usuarios de producción. La identidad usa tres cuentas ficticias (`juan`, `maria` y `david_rodriguez_espinosa`); únicamente para laboratorio local o red aislada. No desplegar en Internet.
3. **Compatibilidad:** el parser de solicitudes admite solo objetos JSON planos de cadenas o números y los identificadores definidos arriba. Es una restricción explicitada para la versión implementada; no pretende ser un parser JSON universal.
4. **Demo visual:** el cliente de terminal imprime JSON legible; la GUI se menciona como recomendación, no obligación, en el enunciado.
5. **Simulación:** `SMDP_DROP_FIRST_ACK=1` descarta deliberadamente el primer ACK de cada nuevo EVENT para demostrar la recuperación. Mantener esta bandera desactivada en ejecuciones habituales.

Las limitaciones se declaran para evitar confundir una entrega académica con una plataforma de supervisión segura o tolerante a fallos de almacenamiento.
