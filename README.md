# Sistema de monitoreo y control distribuido — SMDP/1.0

Proyecto de Internet: Arquitectura y Protocolos (Telemática, 2026-2).

La arquitectura se diseñó en **Fase 1**; la implementación inicial de sockets está en **Fase 2**; los mecanismos de concurrencia, recuperación de errores y pruebas se incorporan en **Fase 3**. Se preservan las fases anteriores para evidenciar la evolución del repositorio.

## Estructura

- `Fase1/`: PDF original de diseño (se conserva en el repositorio existente).
- `Fase2/`: implementación inicial (se conserva, sin reemplazar).
- `Fase3/server/`: servidor principal en C (sockets Berkeley TCP y UDP), parser de SMDP y servicio de identidad **independiente** escrito en C para laboratorio.
- `Fase3/clientes/`: simulador de nodos y cliente administrativo en Python.
- `Fase3/tests/`: pruebas de integración y unidad (solo biblioteca estándar).
- `Fase3/PROTOCOLO_SMDP.md`: especificación implementable del protocolo y decisiones de diseño.
- `Fase3/README.md`: instrucciones de compilación, ejecución, pruebas, demostración y límites conocidos.
- `Fase3/CAMBIOS_RESPECTO_FASE2.md`: evolución de la implementación.
- `Fase3/GUIA_SUSTENTACION.md`: preguntas y funciones relevantes para la defensa oral.

## Ejecución en Linux o Ubuntu/WSL

```bash
cd Fase3/server
make
./identidad 6001              # terminal 1: servicio de identidad de laboratorio
./servidor 6000 servidor.log   # terminal 2: servidor central
```

En otras terminales, desde `Fase3/clientes`:

```bash
python3 node_client.py localhost 6000 nodo-01
python3 node_client.py localhost 6000 nodo-02
python3 admin_client.py localhost 6000 juan --node nodo-01 --resource all
```

La clave de **demostración** de `juan` es `1234`, solicitada mediante prompt. `maria` utiliza la misma clave y tiene perfil `VISOR`.

## Pruebas

```bash
cd Fase3/server && make && cd ..
python3 -m unittest discover -s tests -v
```

Para una demostración controlada de retransmisiones: iniciar **solo el servidor** con `SMDP_DROP_FIRST_ACK=1 ./servidor 6000 servidor.log` y ejecutar un nodo. El servidor simula la pérdida del primer ACK de cada evento nuevo, para comprobar el reintento y la deduplicación.

**Seguridad:** los usuarios y las claves son ficticios y únicamente para laboratorio; las comunicaciones aún no cifran credenciales ni tokens. No desplegar este código directamente en Internet ni utilizar contraseñas reales. Consulte `Fase3/README.md`.
