# Fase 2: Implementación de comunicación básica
## Sistema de monitoreo y control distribuido (protocolo SMDP/1.0)

Esta entrega implementa la comunicación básica descrita en el diseño de la Fase 1,
siguiendo exactamente la especificación del protocolo **SMDP/1.0**: formato de
mensaje, tipos de mensaje, códigos de error y reglas de transporte (TCP para
`REG`/`AUTH`/`QUERY`, UDP para `STATUS`/`EVENT`→`ACK`).

## Contenido de la entrega

```
fase2/
├── server/
│   ├── server.c      -> servidor central en C, API de sockets Berkeley
│   └── Makefile
├── clientes/
│   ├── node_client.py   -> simula un nodo (REG, STATUS, EVENT)
│   └── admin_client.py  -> simula un cliente de administración (AUTH, QUERY)
└── README.md
```

## Qué quedó implementado en esta fase

- Creación y configuración de los sockets TCP y UDP con la API de Berkeley
  (`socket()`, `bind()`, `listen()`, `accept()` / `connect()`, `send()`,
  `recv()`, `sendto()`, `recvfrom()`).
- Establecimiento de la comunicación: registro de nodos (`REG`/`REG_OK`) y
  autenticación de clientes (`AUTH`/`AUTH_OK`/`AUTH_ERR`).
- Envío y recepción de mensajes: `STATUS` y `EVENT` por UDP, `QUERY`/`QUERY_RESP`
  por TCP.
- Interpretación de mensajes según el formato
  `SMDP/1.0|TIPO|ORIGEN|SEQ|TIMESTAMP|TOKEN|PAYLOAD_JSON`, con un parser propio
  (sin librerías externas) para el JSON simple del `PAYLOAD`.
- Construcción de respuestas siguiendo el mismo formato, incluyendo los códigos
  de error definidos en el diseño (`MALFORMED_MESSAGE`, `UNAUTHORIZED`,
  `NOT_FOUND`, `NODE_NOT_REGISTERED`, `UNKNOWN_TYPE`).
- Reglas básicas del protocolo: un nodo debe registrarse antes de enviar
  `STATUS`/`EVENT`, un cliente debe autenticarse antes de hacer `QUERY`, cada
  `EVENT` recibe `ACK` y los duplicados se detectan por número de secuencia.
- Resolución de nombres en los clientes con `getaddrinfo` (vía
  `socket.getaddrinfo` en Python), con reintentos y espera creciente si la
  resolución falla, sin terminar el programa.
- Concurrencia mínima en el servidor: un hilo (`pthread`) por conexión TCP y un
  hilo separado para el socket UDP, con mutex protegiendo las tablas
  compartidas de nodos y sesiones.
- Logging a consola y a archivo con fecha, transporte, IP y puerto de origen,
  tipo de mensaje, secuencia, identidad y resultado.

Lo que el diseño deja para la Fase 3 (y que aquí se dejó simplificado a propósito):
el servicio de identidad separado se representa con una tabla de usuarios de
prueba dentro del mismo servidor (`usuarios[]` en `server.c`), la resiliencia
ante fallas concurrentes más exigentes, y las pruebas con múltiples nodos y
clientes simultáneos.

## Compilar el servidor

```bash
cd server
make
```

Esto genera el ejecutable `servidor`.

## Ejecutar el servidor

```bash
./servidor <puerto> <archivo_de_logs>
# ejemplo:
./servidor 6000 servidor.log
```

El servidor queda escuchando en ese puerto tanto en TCP como en UDP (dos
sockets distintos que pueden compartir el mismo número de puerto porque
pertenecen a transportes diferentes).

## Ejecutar los clientes de prueba

En otra terminal (o en otra máquina, apuntando al nombre de dominio del
servidor en vez de `localhost`):

```bash
cd clientes

# Simula un nodo que se registra y reporta estado/eventos
python3 node_client.py localhost 6000 nodo-03

# Simula un cliente de administración autenticado
python3 admin_client.py localhost 6000 juan 1234
```

Usuarios de prueba definidos en el servidor para esta fase:

| Usuario | Clave | Perfil |
|---|---|---|
| juan  | 1234 | ADMIN |
| maria | 1234 | VISOR |

Al ejecutar `admin_client.py` se pide el identificador del nodo a consultar
(por ejemplo `nodo-03`, el mismo que registró `node_client.py`).

## Notas sobre la resolución de nombres

Para la demostración se puede usar `localhost` directamente. Si el grupo ya
tiene montado el servidor DNS propio de la Fase 1 (bind9, con una zona que
resuelva algo como `servidor.monitoreo.local`), basta con pasar ese nombre de
dominio en vez de `localhost` a cualquiera de los dos clientes; el código ya
resuelve el nombre con `getaddrinfo` y reintenta si la resolución falla.

## Verificación rápida de lo pedido en el enunciado

- [x] Creación y configuración de los sockets.
- [x] Establecimiento de la comunicación (REG y AUTH).
- [x] Envío de mensajes (STATUS, EVENT, QUERY).
- [x] Recepción de mensajes.
- [x] Interpretación de mensajes (parser de SMDP/1.0).
- [x] Construcción de respuestas (REG_OK, AUTH_OK/ERR, ACK, QUERY_RESP, ERROR).
- [x] Implementación de las reglas básicas del protocolo.
