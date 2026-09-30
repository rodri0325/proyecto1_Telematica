# Fase 3 — Concurrencia, resiliencia y pruebas

Este directorio es una **evolución separada** de la Fase 2; no elimina los archivos de la entrega anterior. Corresponde a la Fase 3 del enunciado «Entrega 1 - sockets»: concurrencia, errores, excepciones, duplicados, pérdida de ACK y pruebas defendibles en sustentación.

## 1. Archivos

```text
Fase3/
├── server/
│   ├── server.c             Servidor SMDP/1.0, Berkeley TCP y UDP
│   ├── protocol.h           Interfaz del parser
│   ├── protocol.c           Encabezados y validador JSON plano/UTF-8
│   ├── identity_service.c   Servicio de identidad de laboratorio independiente
│   └── Makefile             Compila 'servidor' e 'identidad'
├── clientes/
│   ├── common.py            DNS, TCP por líneas, mensajes
│   ├── node_client.py       Registro, STATUS y EVENT/retransmisión/cola pendiente
│   └── admin_client.py      AUTH, status, history, events, nodes
├── tests/
│   ├── test_common.py       Parser, enmarcado y excepción DNS simulada
│   └── test_integration.py  Sockets reales: flujo principal y fallos
├── PROTOCOLO_SMDP.md        Especificación técnica implementada
└── README.md
```

## 2. Instalación y ejecución (Ubuntu o WSL)

Requiere GCC, Make y Python 3, sin librerías externas.

```bash
# Dentro de la raíz del repositorio:
cd Fase3/server
make
```

Abrir cuatro terminales en el directorio raíz del repositorio:

**Terminal 1 — servicio de identidad:**

```bash
cd Fase3/server
./identidad 6001
```

**Terminal 2 — servidor central:**

```bash
cd Fase3/server
./servidor 6000 servidor.log
```

**Terminales 3 y 4 — iniciar los nodos casi al mismo tiempo:**

```bash
cd Fase3/clientes
python3 node_client.py localhost 6000 nodo-01
```

```bash
cd Fase3/clientes
python3 node_client.py localhost 6000 nodo-02
```

Para consultar ambos nodos, utilizar una quinta terminal:

```bash
cd Fase3/clientes
python3 admin_client.py localhost 6000 juan --node nodo-01 --resource all
python3 admin_client.py localhost 6000 juan --node nodo-02 --resource all
```

El cliente pide la contraseña mediante `getpass`; para los usuarios ficticios `juan` (ADMIN) y `maria` (VISOR), la contraseña de **laboratorio** es `1234`. La salida administrativa incluye estado instantáneo, cinco muestras de historial y eventos aceptados.

**Roles:** ejecutar `python3 admin_client.py localhost 6000 maria --resource nodes`; debe devolver `UNAUTHORIZED`, mientras que con `juan` se permite el listado (sin necesitar `--node`).

## 3. Pruebas automáticas

```bash
cd Fase3/server && make && cd ..
python3 -m unittest discover -s tests -v
```

El conjunto de pruebas crea instancias independientes del servidor y del servicio de identidad con puertos libres. Comprueba conexiones, registro de varios nodos, enmarcado TCP parcial, varias respuestas por conexión, validación de mensajes, límites de UDP, cinco muestras históricas, STATUS viejo, retransmisión con pérdida forzada del primer ACK, deduplicación, autenticación, roles, ocho consultas concurrentes, desconexiones y logs. Prueba además la recuperación DNS del cliente con una falla simulada.

Este conjunto utiliza **sockets y procesos reales** para las pruebas de integración. Los datos y registros temporales de pruebas se eliminan automáticamente.

## 4. Prueba manual de pérdida de un ACK

Detener el servidor (no es necesario detener identidad) y volver a iniciarlo así:

```bash
cd Fase3/server
SMDP_DROP_FIRST_ACK=1 ./servidor 6000 servidor.log
```

Iniciar `node_client.py localhost 6000 nodo-03`. En la salida del nodo debe aparecer `EVENT intento=1`, `sin ACK`, `EVENT intento=2` y finalmente `ACK confirmado`. En el servidor debe aparecer `SIMULATED_LOSS` y solo una entrada de evento nuevo. Consultar `events` con `juan` para comprobar que hay **una** entrada, no dos.

**Importante:** `SMDP_DROP_FIRST_ACK` es una bandera exclusivamente de pruebas. No modifica el transporte real; simula pérdida del ACK en la capa de aplicación.

## 5. Reproducción de fallos y posibles preguntas del profesor

- **TCP fragmentado:** `tcp_worker()` acumula bytes hasta LF; la prueba `test_04_malformed_unknown_version_and_tcp_framing` envía `REG` en dos trozos y dos peticiones en un solo `sendall`.
- **Concurrencia:** `main()` crea un hilo TCP por cliente y un receptor más tres trabajadores UDP; `shard_for()` asigna un mismo nodo a una misma cola FIFO. Los mutex protegen nodos, sesiones y registros.
- **Duplicados:** el servidor almacena EVENT antes del ACK y recuerda 64 secuencias recientes por nodo; el nodo conserva localmente los eventos pendientes hasta un ACK correlacionado.
- **Mensajes incorrectos:** `smdp_parse()` valida la cabecera y la versión; `json_parse_object()` valida el subconjunto de JSON de esta versión; los errores producen una respuesta sin detener el servidor.
- **DNS:** `resolve()` vuelve a llamar a `getaddrinfo` tras errores; la prueba DNS inyecta un error inicial controlado. Para demostrar DNS real, sustituir `localhost` por un nombre que resuelva mediante el DNS disponible en la red de laboratorio.
- **Logging:** consultar `server/servidor.log` para solicitudes y respuestas; credenciales y tokens se redactan omitiéndolos por completo.
- **Perfiles:** identidad y supervisión son **dos procesos**; el servidor realiza `getaddrinfo()` del servicio de identidad antes de aceptar AUTH. Los recursos `nodes` y `diagnostic` requieren ADMIN.

### Capturas sugeridas para evidencias

1. Dos terminales de nodo con `REG_OK`, `STATUS` y `EVENT`.
2. Terminal servidor con registros TCP y UDP de los dos nodos y del administrador.
3. Dos consultas administrativas: `status`, `history` (cinco muestras) y `events`.
4. Log de pérdida `SIMULATED_LOSS`, segundo intento del nodo y un solo evento consultado.
5. Salida de `python3 -m unittest discover -s tests -v` con todas las pruebas correctas.

No subir contraseñas reales, tokens ni registros de redes privadas sin revisar.

## 6. Cobertura del enunciado

| Enunciado | Dónde se evidencia |
|---|---|
| Servidor principal **C**, API Berkeley, puerto y archivo de logs | `server/server.c`, `server/Makefile`; consola y archivo |
| Dos nodos, servidor y cliente(s), sin comunicación directa nodo-cliente | `clientes/node_client.py`, `clientes/admin_client.py`; pruebas de registro concurrente |
| Crear, configurar, recibir, enviar, interpretar y responder | `socket/bind/listen/accept/recv/send`, `sendto/recvfrom`, `protocol.c` |
| DNS sin direcciones IP literales en aplicación | `getaddrinfo()` en clientes y servidor hacia identidad |
| Mostrar estado y cinco históricos | `admin_client.py` (`status`, `history`) y anillo de cinco muestras |
| Autenticación y perfiles | Servicio separado `identity_service.c`, token de sesión y permisos |
| Múltiples consultas simultáneas | Hilo TCP por conexión, test concurrente de ocho clientes |
| Excepciones, fallos, duplicados y pérdidas | Parser estricto, errores SMDP, colas FIFO, ACK y reintentos, pruebas automáticas |
| Registro de peticiones/respuestas IP/puerto origen | `log_line()` sincronizada por mutex |
| Especificación del protocolo y procedimientos | `PROTOCOLO_SMDP.md`, PDF de Fase 1 preservado |

## 7. Limitaciones demostrables (no ocultar en la sustentación)

- **Autenticación de laboratorio:** el servicio separado incluye dos usuarios ficticios codificados. No hay TLS, LDAP ni identidades reales. No desplegar en Internet.
- **Persistencia:** las cinco muestras y los últimos 32 eventos de cada nodo residen en RAM. Los eventos del **cliente** sin ACK se guardan en disco local en `clientes/.pending/`, pero el servidor no es una base de datos duradera; la entrega exactamente una vez ante fallos/reinicios no está garantizada.
- **Capacidad:** máximo 64 nodos, 64 sesiones simultáneas y 32 eventos consultables por nodo; deduplicación de las 64 secuencias recientes por nodo. `QUERY nodes` admite `limit` y `offset`.
- **Interfaz:** se usa terminal con JSON legible. El enunciado recomienda, pero no exige, GUI.
- **IPv4:** el servidor principal y sus clientes UDP utilizan IPv4; el protocolo y la resolución por nombres se mantienen. No se codifican direcciones IP en la aplicación.

**Importante antes de la entrega:** compilar y ejecutar estas pruebas también en el entorno final de la sustentación. No afirmar que son funcionalidades de producción ni que se probaron en una infraestructura DNS remota sin haberlo hecho realmente.
