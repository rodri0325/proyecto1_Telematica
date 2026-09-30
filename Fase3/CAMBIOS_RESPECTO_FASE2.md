# Evolución propuesta desde Fase 2 hacia Fase 3

La versión de Fase 2 que se consultó en el repositorio `rodri0325/proyecto1_Telematica` contiene `server/server.c`, `node_client.py`, `admin_client.py` y un README de comunicación básica. Este paquete **no sobrescribe Fase 2**: crea una carpeta Fase 3 independiente para hacer visible la evolución de la implementación.

| Aspecto | Código de Fase 2 | Complemento de Fase 3 | Comprobación |
|---|---|---|---|
| Parser | Separación básica por `|` y búsqueda de fragmentos JSON | Cabecera, versión, secuencia, token, UTF-8 y JSON plano validados | Entradas malformadas, versión inválida, parámetros fuera de rango |
| Enmarcado TCP | Servidor reconstruía LF; clientes asumían respuesta completa en un `recv` | Servidor con overflow recuperable; clientes leen hasta LF | Petición partida y peticiones unidas en un único envío |
| Concurrencia | Hilo por conexión TCP, un receptor UDP | TCP paralelo + receptor UDP + 3 trabajadores con cola FIFO por nodo | Ocho clientes concurrentes, dos nodos simultáneos |
| Almacenamiento | Últimas cinco muestras | Cinco muestras con descarte de STATUS antiguos y hasta 32 eventos por nodo | Consulta de historiales y eventos |
| EVENT | ACK y deduplicación basada solo en el evento previo | Detección de 64 secuencias recientes por nodo, almacenamiento previo al ACK, simulación pérdida, tres retransmisiones | Primer ACK perdido, siguiente retransmisión, un evento registrado |
| Recuperación de nodo | DNS con reintentos, tres intentos de envío | DNS, reconexión de REG, EVENT pendiente guardado localmente hasta ACK | Pruebas del flujo y DNS inyectado |
| Autenticación | Tabla de usuarios dentro del servidor central | Proceso de identidad independiente (C) consultado por nombre, sesiones con expiración | ADMIN/VISOR, autenticación válida e inválida |
| Autorización | Sesión validada sin diferencia práctica de perfiles | Restricciones `VISOR` frente a `ADMIN` en listado/diagnóstico | `UNAUTHORIZED` para VISOR al consultar `nodes` |
| Logs | Consola y archivo con mutex | Solicitudes y respuestas identificadas, sin credenciales ni tokens; aviso si archivo falla | Comprobación de formato y presencia de entradas TCP/UDP |
| Evidencias | Instrucciones manuales | Pruebas automatizadas, especificación actualizada y guía de sustentación | `python3 -m unittest discover -s tests -v` |

## Orden recomendado de integración real en el repositorio

1. Revisar en equipo el parser y la lógica del servidor (`protocol.*`, `server.c`) y compilar.
2. Integrar el proceso de identidad (`identity_service.c`) y comprobar los perfiles.
3. Integrar y ejecutar los clientes Python; demostrar flujo con dos nodos y un administrador.
4. Ejecutar pruebas automatizadas y corregir en el entorno real cualquier incompatibilidad.
5. Integrar documentación y evidencias verificadas en su propio equipo.

Hacer commits con descripciones que correspondan a las tareas **realmente revisadas y realizadas**. No alterar historial ni inventar ejecuciones para aparentar progreso. Antes de subir, revisar con `git status` y `git diff --stat`; no subir ejecutables, contraseñas, logs privados ni directorios `.pending`.

## Advertencias sobre garantías

El servidor conserva eventos e historiales **en memoria**. La cola de EVENT del nodo es local; la entrega exactamente una vez ante caídas con reinicio del servidor no está resuelta. El servicio de identidad está separado como componente, pero usa cuentas ficticias y protocolos sin cifrar, aptos exclusivamente para un laboratorio aislado. La interfaz es de terminal (el enunciado la admite porque GUI es una recomendación). Para nombres DNS reales fuera de `localhost`, el grupo debe configurarlos y validarlos en su propia red.
