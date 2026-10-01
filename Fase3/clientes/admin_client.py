#!/usr/bin/env python3
"""Cliente administrativo SMDP/1.0 con consultas y lectura TCP segura por líneas."""
import argparse
import getpass
import json
import sys
import time
from common import connect_retry, message, recv_frame


def query_one(reader, sock, username, token, node, resource, seq, limit=5):
    data = {"resource": resource, "limit": limit}
    if resource not in ("nodes", "diagnostic"):
        data["node"] = node
    sock.sendall(message("QUERY", username, seq, token, data))
    result = recv_frame(reader)
    if result["seq"] != seq:
        raise ValueError(f"Respuesta con secuencia inesperada {result['seq']}")
    if result["kind"] == "ERROR":
        print(f"[QUERY {resource}] ERROR {result['data']}", flush=True)
        return None
    if result["kind"] != "QUERY_RESP":
        raise ValueError(f"Respuesta inesperada: {result['kind']}")
    print(f"[QUERY {resource}] {json.dumps(result['data'], ensure_ascii=False, indent=2)}", flush=True)
    return result["data"]


def main():
    parser = argparse.ArgumentParser(description="Cliente SMDP/1.0")
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("username")
    # Compatibilidad con la CLI anterior, aunque se recomienda getpass.
    parser.add_argument("legacy_password", nargs="?", default=None)
    parser.add_argument("--node", default=None)
    parser.add_argument("--resource", choices=("status", "history", "events", "nodes", "diagnostic", "all"),
                        default="all")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    password = args.legacy_password or getpass.getpass("Clave: ")
    if args.legacy_password:
        print("[AVISO] Para evitar claves en el historial de comandos, omite la clave y usa el prompt.")
    if not 1 <= args.limit <= 10:
        parser.error("--limit debe estar entre 1 y 10")
    node = args.node
    if not node and args.resource not in ("nodes", "diagnostic"):
        node = input("Nodo a consultar (ej. nodo-01): ").strip() or "nodo-01"
    # Un error TCP exige reconectar y autenticar de nuevo. No asumir recv completo.
    while True:
        try:
            with connect_retry(args.host, args.port) as sock:
                with sock.makefile("rb") as reader:
                    sock.sendall(message("AUTH", args.username, 1, "-", {"user": args.username, "pass": password}))
                    reply = recv_frame(reader)
                    if reply["kind"] != "AUTH_OK":
                        print(f"[AUTH] rechazada: {reply['data']}")
                        return 1
                    token = reply["token"]
                    print(f"[AUTH] autenticado, perfil={reply['data'].get('profile')}")
                    resources = ("status", "history", "events") if args.resource == "all" else (args.resource,)
                    for seq, resource in enumerate(resources, start=2):
                        query_one(reader, sock, args.username, token, node, resource, seq, args.limit)
                    return 0
        except (OSError, ConnectionError, ValueError) as exc:
            print(f"[RECUPERACIÓN TCP] {exc}; reconectando y autenticando", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    sys.exit(main())
