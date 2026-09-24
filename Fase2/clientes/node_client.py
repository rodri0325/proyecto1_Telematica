#!/usr/bin/env python3
"""
Cliente de nodo - Sistema de monitoreo y control distribuido
Protocolo: SMDP/1.0

Simula un nodo de monitoreo:
  1. Resuelve el nombre del servidor por DNS (getaddrinfo), con reintentos
     con backoff (1, 2, 4, 8, 30 s) si la resolucion falla.
  2. Abre una conexion TCP, envia REG y recibe REG_OK con un token.
  3. Envia periodicamente STATUS por UDP usando ese token.
  4. De vez en cuando simula un EVENT critico y espera el ACK del
     servidor, reintentando hasta 3 veces si no llega a tiempo.

Uso:
    python3 node_client.py <host_servidor> <puerto> <id_nodo>
"""

import socket
import sys
import time
import json
import random

VERSION = "SMDP/1.0"


def resolver_host(host):
    """Resuelve el nombre del servidor con reintentos y backoff, sin
    dejar caer el programa si la resolucion falla."""
    esperas = [1, 2, 4, 8, 30]
    intento = 0
    while True:
        try:
            info = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
            ip = info[0][4][0]
            print(f"[DNS] {host} resuelto a {ip}")
            return ip
        except socket.gaierror as e:
            espera = esperas[min(intento, len(esperas) - 1)]
            print(f"[DNS] fallo al resolver '{host}' ({e}). Reintentando en {espera}s...")
            time.sleep(espera)
            intento += 1


def construir_mensaje(tipo, origen, seq, token, payload_dict):
    payload = json.dumps(payload_dict, separators=(",", ":"))
    ts = int(time.time())
    return f"{VERSION}|{tipo}|{origen}|{seq}|{ts}|{token}|{payload}\n"


def parsear_mensaje(linea):
    partes = linea.strip("\n").split("|", 6)
    if len(partes) < 7 or partes[0] != VERSION:
        return None
    campos = ["version", "tipo", "origen", "seq", "timestamp", "token", "payload"]
    return dict(zip(campos, partes))


def registrar_nodo(ip, puerto, id_nodo):
    """Abre TCP, envia REG y devuelve el token asignado por el servidor."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, puerto))
        msg = construir_mensaje("REG", id_nodo, 1, "-",
                                 {"device_type": "sensor", "location": "sala1"})
        s.sendall(msg.encode())
        respuesta = s.recv(4096).decode()
        m = parsear_mensaje(respuesta)
        if not m or m["tipo"] != "REG_OK":
            raise RuntimeError(f"Registro fallido: {respuesta.strip()}")
        print(f"[TCP] registrado como '{id_nodo}', token={m['token']}")
        return m["token"]


def enviar_status(sock_udp, ip, puerto, id_nodo, token, seq):
    payload = {
        "cpu": random.randint(10, 90),
        "temp": round(random.uniform(30, 60), 1),
        "battery": random.randint(20, 100),
        "state": "ok",
    }
    msg = construir_mensaje("STATUS", id_nodo, seq, token, payload)
    sock_udp.sendto(msg.encode(), (ip, puerto))
    print(f"[UDP] STATUS seq={seq} -> {payload}")


def enviar_event_con_reintentos(sock_udp, ip, puerto, id_nodo, token, seq):
    payload = {"event": "threshold", "value": 95, "threshold": 90}
    msg = construir_mensaje("EVENT", id_nodo, seq, token, payload)

    sock_udp.settimeout(1.0)
    for intento in range(1, 4):
        sock_udp.sendto(msg.encode(), (ip, puerto))
        print(f"[UDP] EVENT seq={seq} enviado (intento {intento})")
        try:
            data, _ = sock_udp.recvfrom(4096)
            m = parsear_mensaje(data.decode())
            if m and m["tipo"] == "ACK" and m["seq"] == str(seq):
                print(f"[UDP] ACK recibido para EVENT seq={seq}")
                return True
        except socket.timeout:
            print(f"[UDP] timeout esperando ACK de seq={seq}")
    print(f"[UDP] no se confirmo el EVENT seq={seq} tras 3 intentos")
    return False


def main():
    if len(sys.argv) != 4:
        print(f"Uso: {sys.argv[0]} <host_servidor> <puerto> <id_nodo>")
        sys.exit(1)

    host, puerto, id_nodo = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    ip = resolver_host(host)

    token = registrar_nodo(ip, puerto, id_nodo)

    sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seq = 2
    ciclos = 5
    for i in range(ciclos):
        enviar_status(sock_udp, ip, puerto, id_nodo, token, seq)
        seq += 1
        if i == 2:  # a mitad de la demo, simula un evento critico
            enviar_event_con_reintentos(sock_udp, ip, puerto, id_nodo, token, seq)
            seq += 1
        time.sleep(2)

    print("[nodo] demostracion finalizada")


if __name__ == "__main__":
    main()
