#!/usr/bin/env python3
"""Cliente de prueba para autenticarse y consultar un nodo."""

import socket
import sys
import time
import json

VERSION = "SMDP/1.0"


def resolver_host(host):
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


def main():
    if len(sys.argv) != 5:
        print(f"Uso: {sys.argv[0]} <host_servidor> <puerto> <usuario> <clave>")
        sys.exit(1)

    host, puerto, usuario, clave = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    ip = resolver_host(host)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, puerto))

    # Autenticacion
    msg = construir_mensaje("AUTH", usuario, 1, "-", {"user": usuario, "pass": clave})
    s.sendall(msg.encode())
    resp = parsear_mensaje(s.recv(4096).decode())

    if not resp or resp["tipo"] != "AUTH_OK":
        print(f"[AUTH] rechazada: {resp}")
        s.close()
        sys.exit(1)

    token = resp["token"]
    print(f"[AUTH] sesion iniciada, perfil={resp['payload']}, token={token}")

    # Consulta del estado actual y del historial
    nodo_objetivo = input("Nodo a consultar (ej. nodo-03): ").strip() or "nodo-03"

    for recurso, seq in (("status", 2), ("history", 3)):
        msg = construir_mensaje("QUERY", usuario, seq, token,
                                 {"node": nodo_objetivo, "resource": recurso, "limit": 5})
        s.sendall(msg.encode())
        resp = parsear_mensaje(s.recv(4096).decode())
        if resp and resp["tipo"] == "QUERY_RESP":
            datos = json.loads(resp["payload"])
            print(f"[QUERY {recurso}] {json.dumps(datos, indent=2, ensure_ascii=False)}")
        else:
            print(f"[QUERY {recurso}] error: {resp}")

    s.close()


if __name__ == "__main__":
    main()
