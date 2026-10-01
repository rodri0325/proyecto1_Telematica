#!/usr/bin/env python3
"""Nodo SMDP/1.0: registro TCP, telemetría UDP y EVENT con ACK/reintentos."""
import json
import os
import random
import socket
import sys
import time
from pathlib import Path
from common import SAFE_ID, connect_retry, message, parse, recv_frame, resolve, ProtocolError


def register(host, port, node_id):
    while True:
        with connect_retry(host, port) as sock:
            try:
                sock.sendall(message("REG", node_id, 1, "-", {
                    "device_type": "sensor", "location": "sala1"}))
                with sock.makefile("rb") as reader:
                    response = recv_frame(reader)
                if response["kind"] != "REG_OK":
                    raise ProtocolError(f"REG rechazado: {response}")
                print(f"[TCP] nodo {node_id}: REG_OK", flush=True)
                return response["token"]
            except (OSError, ConnectionError, ProtocolError) as exc:
                print(f"[TCP] registro fallido: {exc}; reintentando", flush=True)
                time.sleep(2)


def udp_socket(host, port):
    addresses = resolve(host, port, socket.SOCK_DGRAM, socket.AF_INET)
    family, kind, proto, _, address = addresses[0]
    sock = socket.socket(family, kind, proto)
    sock.settimeout(1)
    return sock, address


def send_event(sock, address, node_id, token, seq, data):
    packet = message("EVENT", node_id, seq, token, data)
    for attempt in range(1, 5):  # envío original + 3 retransmisiones
        try:
            sock.sendto(packet, address)
        except OSError as exc:
            print(f"[UDP] fallo al enviar EVENT, reintento posterior: {exc}", flush=True)
            time.sleep(1)
            continue
        print(f"[UDP] EVENT seq={seq} intento={attempt}", flush=True)
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            sock.settimeout(max(0.01, deadline - time.monotonic()))
            try:
                raw, sender = sock.recvfrom(4097)
                if sender != address:
                    continue  # no aceptar un ACK de otro remitente
                response = parse(raw)
                if response["seq"] != seq:
                    continue  # ACK atrasado de otro EVENT
                if response["kind"] == "ACK" and response["token"] == token:
                    print(f"[UDP] ACK confirmado para seq={seq}", flush=True)
                    return True
                if response["kind"] == "ERROR" and response["data"].get("code") == "NODE_NOT_REGISTERED":
                    raise ProtocolError("TOKEN_RECHAZADO")
            except socket.timeout:
                break
            except ProtocolError as exc:
                if str(exc) == "TOKEN_RECHAZADO":
                    raise
                print(f"[UDP] respuesta inválida ignorada: {exc}", flush=True)
        print(f"[UDP] sin ACK seq={seq} después de 1s", flush=True)
    return False


def spool_file(node_id):
    base = Path(os.environ.get("SMDP_PENDING_DIR", str(Path(__file__).parent / ".pending")))
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{node_id}.json"


def persist_pending(path, pending):
    """Cola de EVENT pendientes en disco: no sobrescribir eventos sin ACK."""
    if not pending:
        path.unlink(missing_ok=True)
        return
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(pending, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)


def read_pending(path):
    if not path.exists():
        return []
    content = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(content, dict):  # compatibilidad con primera versión
        content = [content]
    if not isinstance(content, list) or any(not isinstance(item, dict) or "event" not in item
                                               for item in content):
        raise ValueError("Formato de la cola de eventos no válido")
    return content


def flush_pending(sock, address, host, port, node_id, token, seq, pending, path):
    while pending:
        event = pending[0]
        try:
            confirmed = send_event(sock, address, node_id, token, seq, event)
        except ProtocolError as exc:
            if str(exc) != "TOKEN_RECHAZADO":
                raise
            token = register(host, port, node_id)
            seq = 2
            confirmed = send_event(sock, address, node_id, token, seq, event)
        seq += 1
        if not confirmed:
            print(f"[PENDIENTE] EVENT sin ACK, queda guardado en {path}", flush=True)
            break
        pending.pop(0)
        persist_pending(path, pending)
    return token, seq


def main():
    if len(sys.argv) != 4 or not SAFE_ID.fullmatch(sys.argv[3]):
        print(f"Uso: {sys.argv[0]} <host_servidor> <puerto> <id_nodo>")
        return 2
    host, port, node_id = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    path = spool_file(node_id)
    token = register(host, port, node_id)
    sock, address = udp_socket(host, port)
    seq = 2
    try:
        pending = read_pending(path)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"[ERROR] Revisar cola de eventos {path}: {exc}. No se sobrescribe.")
        return 1
    if pending:
        print(f"[RECUPERACION] reintentando {len(pending)} EVENT pendientes", flush=True)
        token, seq = flush_pending(sock, address, host, port, node_id,
                                   token, seq, pending, path)
    for cycle in range(5):
        data = {"cpu": random.randint(10, 90), "temp": round(random.uniform(30, 60), 1),
                "battery": random.randint(20, 100), "state": "ok"}
        try:
            sock.sendto(message("STATUS", node_id, seq, token, data), address)
            print(f"[UDP] STATUS seq={seq} {data}", flush=True)
        except OSError as exc:
            print(f"[UDP] STATUS no enviado seq={seq}: {exc}", flush=True)
        seq += 1
        if cycle == 2:
            event = {"event": "threshold", "value": 95, "threshold": 90}
            pending.append(event)
            persist_pending(path, pending)
            if len(pending) == 1:
                token, seq = flush_pending(sock, address, host, port, node_id,
                                           token, seq, pending, path)
            else:
                # No cambiar la secuencia de un evento antiguo sin ACK en la
                # misma sesión; dejar la cola íntegra para el próximo registro.
                print(f"[PENDIENTE] {len(pending)} EVENT conservados para próxima ejecución")
        time.sleep(2)
    sock.close()
    print("[nodo] demostración terminada", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
