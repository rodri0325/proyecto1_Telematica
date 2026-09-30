# Guía de sustentación del proyecto SMDP/1.0

El profesor exige explicar el código y las decisiones técnicas. Esta guía enlaza lo escrito en el PDF de la Fase 1 con funciones concretas de la implementación de Fase 3. **Lean el código y ejecuten las pruebas antes de entregar: la guía no sustituye comprenderlo.**

## 1. Arquitectura para explicar en un minuto

- Dos nodos se registran por TCP, obtienen tokens de registro y envían `STATUS` periódicos por UDP y `EVENT` críticos con ACK.
- El servidor central está íntegramente en C: un socket de escucha TCP con un hilo por conexión, otro socket UDP con un receptor y tres trabajadores.
- Los clientes administrativos consultan estado, historial y eventos al servidor por TCP. No existe conexión directa con los nodos.
- La identidad se comprueba consultando por nombre un **segundo servicio en C**, limitado a un laboratorio local. `VISOR` consulta; `ADMIN` puede además consultar la lista/diagnóstico de nodos.

Diagrama breve:

```text
Nodo A ---- TCP REG ------------------> Servidor C <---- TCP AUTH/QUERY ---- Cliente admin
       ---- UDP STATUS/EVENT -------->     |  ^
       <--- UDP ACK para EVENT -------     |  | TCP privado
Nodo B ---- TCP/UDP ------------------>     v  |
                                       Identidad C
```

## 2. Preguntas frecuentes y respuestas apoyadas en código

**¿Por qué dos protocolos?** `REG`, `AUTH` y `QUERY` usan TCP: entrega y orden son importantes. `STATUS` cambia pronto y tolera pérdidas, por eso usa UDP sin ACK. `EVENT` necesita confirmación: usa UDP para mantener la misma ruta de telemetría, pero añade secuencia, ACK, timeout y retransmisión. Corresponde a la tabla de selección de transportes del PDF de Fase 1.

**¿Dónde están los sockets Berkeley?** En `server/server.c`, `main()`: `socket`, `setsockopt`, `bind`, `listen`, `accept`. En `udp_receiver()` y `udp_reply()`: `recvfrom` y `sendto`. El proceso `identity_service.c` también utiliza exclusivamente sockets Berkeley.

**¿Por qué pueden TCP y UDP compartir el puerto 6000?** Son transportes distintos; el sistema operativo distingue TCP/6000 y UDP/6000. Hay sockets separados con direcciones locales compatibles.

**¿Cómo manejan la concurrencia?** Cada `accept()` TCP crea un hilo con `pthread_create()`. UDP tiene un receptor y tres trabajadores. `shard_for()` distribuye según ORIGEN y asigna siempre el mismo nodo a la misma cola FIFO. Los mutex protegen las estructuras comunes de nodos, sesiones, colas y logs; así un cliente no bloquea la atención de todos los demás.

**¿Por qué no basta una llamada a `recv()`?** TCP entrega un flujo continuo de bytes, no los mensajes originales. `tcp_worker()` construye líneas LF antes de invocar `tcp_dispatch()`; también conserva varias líneas concatenadas. En Python, `common.recv_frame()` usa `readline(4097)`. La prueba de enmarcado envía una petición partida y otras dos pegadas.

**¿Cómo funciona el registro?** `tcp_dispatch()` maneja `REG`, verifica el formato, busca o crea el nodo, genera un token desde `/dev/urandom` y responde `REG_OK`. En un registro nuevo se reinicia el estado de secuencias asociado al token.

**¿Cómo funciona el historial?** La estructura `Node` contiene `history[5]`, `h_next` y `h_count`. Cuando llega un STATUS válido más reciente, se actualiza la posición del anillo y avanza módulo cinco. `QUERY history` reconstruye en orden cronológico las últimas muestras que existen.

**¿Qué pasa si UDP desordena mensajes?** El servidor registra el número máximo de secuencia STATUS aceptado y descarta estados más antiguos; su receptor y colas evitan introducir reordenamiento interno de mensajes del mismo nodo. Como STATUS es frecuente, el último dato es el más útil.

**¿Qué pasa si se pierde un EVENT o su ACK?** El nodo guarda el evento sin confirmar y transmite el mismo EVENT (misma secuencia y token) por UDP. Espera un segundo; puede realizar tres retransmisiones. El servidor almacena el evento antes de enviar ACK. Si llega el EVENT de nuevo, lo reconoce por su secuencia reciente, no lo vuelve a almacenar y reenvía ACK.

**¿Cómo se demuestra la pérdida del ACK?** Iniciar el servidor con `SMDP_DROP_FIRST_ACK=1` y luego ejecutar un nodo. El servidor registra `SIMULATED_LOSS`, el nodo agota el primer timeout, retransmite y finalmente recibe ACK. `QUERY events` entrega una sola entrada.

**¿Cómo implementaron DNS?** Los clientes llaman `getaddrinfo(host, ...)` desde `common.resolve()`. Ante error DNS reintentan con retroceso. El servidor resuelve mediante `getaddrinfo()` el nombre del servicio independiente de identidad. `localhost` es solo una opción de laboratorio; con un nombre DNS del entorno también se puede ejecutar.

**¿Por qué la autenticación no está dentro del servidor central?** El PDF propone separar el servicio de identidad. `server/identity_service.c` es otro ejecutable que recibe una solicitud interna y devuelve el perfil; `validate_with_identity()` en el servidor principal le consulta. No se registran contraseñas. Esta identidad ficticia no reemplaza un proveedor real ni cifra sus intercambios; debe usarse únicamente en laboratorio.

**¿Cómo funcionan los perfiles?** El servidor devuelve un token con `AUTH_OK`; lo conserva con un perfil y una fecha de expiración. `session_profile()` valida el token de cada `QUERY`. `VISOR` consulta `status`, `history` y `events`; `ADMIN` añade `nodes` y `diagnostic`.

**¿Qué sucede con mensajes inválidos?** `smdp_parse()` comprueba versión, cabecera, secuencia y token. `json_parse_object()` verifica el subconjunto JSON admitido. Las funciones de recepción generan un `ERROR` y siguen disponibles para nuevas peticiones. Un cliente que se desconecta libera solo su hilo.

**¿Qué contienen los logs?** Fecha UTC, transporte, IP y puerto del remitente, identidad declarada, dirección (entrada/salida), tipo, secuencia y resultado. Los mutex impiden mezclar líneas; no se registran contraseñas ni tokens.

## 3. Qué ejecutar durante la exposición

1. Compilar con `make` y mostrar que hay dos ejecutables en C (`servidor`, `identidad`).
2. Iniciar la identidad y el servidor con el puerto y el archivo de logs por argumentos.
3. Ejecutar dos nodos casi al mismo tiempo; explicar REG por TCP, STATUS por UDP, EVENT por UDP.
4. Autenticar con `juan` y consultar ambos nodos, con historial de cinco muestras y eventos; probar además `maria` al intentar `nodes`.
5. Demostrar la pérdida del ACK activando la bandera de prueba (requiere reiniciar el servidor y re-registrar nodos).
6. Mostrar las pruebas automáticas: `python3 -m unittest discover -s tests -v`.

## 4. Preguntas que **NO** deben exagerar

- **¿Ya tiene persistencia de base de datos?** No. El servidor mantiene en RAM el historial y los últimos eventos. Solo el simulador de nodos guarda localmente los EVENT que siguen sin ACK.
- **¿Es entrega exactamente una vez?** Se deduplica dentro de una ventana de 64 secuencias recientes por nodo mientras el servidor permanece activo. Una caída o reinicio del servidor puede perder memoria de eventos y deduplicación; no afirmar garantías mayores.
- **¿Es seguro para Internet?** No: la identidad es ficticia, el canal no tiene TLS y los datagramas no tienen autenticación criptográfica. Solo laboratorio/red aislada.
- **¿Usan una GUI?** No. La visualización es una terminal con JSON legible; el enunciado la **recomienda**, pero no obliga una interfaz gráfica.
- **¿Está probado el DNS real en la nube?** Solo se implementó `getaddrinfo()` y se automatizó la recuperación ante un fallo DNS simulado. Si no han configurado/ensayado un dominio real, reconocerlo.

## 5. Distribución de estudio recomendada

El porcentaje de la rúbrica final es **servidor 40%**, **clientes 30%**, **funcionamiento 20%** y **especificación/documentación 10%**. Cada integrante debe conocer el flujo completo; pueden repartirse inicialmente profundizaciones en C/concurrencia y clientes/mensajes, pero ambos deben responder preguntas técnicas sobre decisiones de transporte, excepciones y pruebas.
