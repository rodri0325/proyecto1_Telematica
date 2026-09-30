"""Funciones compartidas de los clientes SMDP/1.0 (TCP por líneas, UDP por datagrama)."""
import json
import re
import socket
import time

VERSION = "SMDP/1.0"
MAX_FRAME = 4096
SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,63}$")


class ProtocolError(Exception):
    pass


def message(kind, origin, seq, token, payload):
    if not SAFE_ID.fullmatch(origin) or not SAFE_ID.fullmatch(kind):
        raise ValueError("Origen o tipo no permitido")
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    text = f"{VERSION}|{kind}|{origin}|{seq}|{int(time.time())}|{token}|{body}\n"
    raw = text.encode("utf-8")
    if len(raw) > MAX_FRAME:
        raise ValueError("Mensaje supera 4096 bytes")
    return raw


def parse(raw):
    if len(raw) > MAX_FRAME or not raw.endswith(b"\n"):
        raise ProtocolError("Mensaje incompleto o demasiado grande")
    try:
        version, kind, origin, seq, ts, token, payload = raw[:-1].decode("utf-8").split("|", 6)
        if version != VERSION or not SAFE_ID.fullmatch(kind) or not SAFE_ID.fullmatch(origin):
            raise ProtocolError("Versión o encabezado incorrecto")
        if not seq.isdecimal() or not ts.isdecimal():
            raise ProtocolError("Secuencia o fecha incorrecta")
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ProtocolError("JSON debe ser un objeto")
        return {"kind": kind, "origin": origin, "seq": int(seq), "ts": int(ts),
                "token": token, "data": data}
    except (ValueError, UnicodeDecodeError) as exc:
        raise ProtocolError(f"No se pudo interpretar la respuesta: {exc}") from exc


def recv_frame(stream):
    """Leer exactamente hasta LF; readline(4097) detecta exceso sin asumir un recv completo."""
    raw = stream.readline(MAX_FRAME + 1)
    if not raw:
        raise ConnectionError("El servidor cerró la conexión")
    return parse(raw)


def resolve(host, port, sock_type, family=socket.AF_UNSPEC):
    """Reintentar DNS sin terminar la aplicación. No se codifica ninguna IP."""
    delays = (1, 2, 4, 8, 30)
    attempt = 0
    while True:
        try:
            addresses = socket.getaddrinfo(host, port, family, sock_type)
            return addresses
        except socket.gaierror as exc:
            pause = delays[min(attempt, len(delays) - 1)]
            print(f"[DNS] {host}: {exc}; reintento en {pause}s", flush=True)
            time.sleep(pause)
            attempt += 1


def connect_retry(host, port):
    delays = (1, 2, 4, 8, 30)
    attempt = 0
    while True:
        last_error = None
        # Probar todas las direcciones antes de concluir que TCP falló.
        for family, kind, proto, _, address in resolve(host, port, socket.SOCK_STREAM):
            sock = socket.socket(family, kind, proto)
            sock.settimeout(5.0)
            try:
                sock.connect(address)
                sock.settimeout(15.0)
                return sock
            except OSError as exc:
                last_error = exc
                sock.close()
        pause = delays[min(attempt, len(delays) - 1)]
        print(f"[TCP] conexión fallida: {last_error}; reintento en {pause}s", flush=True)
        time.sleep(pause)
        attempt += 1
