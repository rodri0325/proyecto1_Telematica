# Fase 2: Comunicación básica
## Sistema de monitoreo y control distribuido

Esta fase contiene una primera versión funcional del protocolo **SMDP/1.0**.
El servidor usa TCP para el registro, la autenticación y las consultas, y UDP
para los reportes de estado y los eventos.

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

## Implementación

- Sockets TCP y UDP usando la API de Berkeley.
- Registro de nodos y autenticacion de usuarios.
- Mensajes `STATUS` y `EVENT` por UDP, con confirmacion `ACK` para los eventos.
- Consultas de estado e historial por TCP.
- Parser sencillo para los mensajes del protocolo y sus respuestas de error.
- Resolucion de nombres con `getaddrinfo` y reintentos en los clientes.
- Un hilo por conexion TCP y un hilo para recibir mensajes UDP.
- Registro de actividad en consola y en el archivo indicado al iniciar el servidor.

Para esta fase, los usuarios se mantienen en una tabla de prueba dentro del
servidor. La separacion del servicio de identidad y las pruebas de mayor carga
quedan para una fase posterior.

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

## Resolucion de nombres

Para la demostración se puede usar `localhost` directamente. Si el grupo ya
tiene montado el servidor DNS propio de la Fase 1 (bind9, con una zona que
resuelva algo como `servidor.monitoreo.local`), basta con pasar ese nombre de
dominio en vez de `localhost` a cualquiera de los dos clientes; el código ya
resuelve el nombre con `getaddrinfo` y reintenta si la resolución falla.

## Lista de comprobacion

- [x] Creación y configuración de los sockets.
- [x] Establecimiento de la comunicación (REG y AUTH).
- [x] Envío de mensajes (STATUS, EVENT, QUERY).
- [x] Recepción de mensajes.
- [x] Interpretación de mensajes (parser de SMDP/1.0).
- [x] Construcción de respuestas (REG_OK, AUTH_OK/ERR, ACK, QUERY_RESP, ERROR).
- [x] Implementación de las reglas básicas del protocolo.
