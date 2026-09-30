#!/usr/bin/env python3
"""Pruebas reproducibles Fase 3 (stdlib): sockets reales y procesos aislados."""
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / "clientes"))
from common import message, parse, recv_frame  # noqa: E402


def free_port():
    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


class ServerIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix="smdp_tests_")
        cls.log = Path(cls.tmp.name) / "server.log"
        cls.out = open(Path(cls.tmp.name) / "output.log", "w+", encoding="utf8")
        cls.id_out = open(Path(cls.tmp.name) / "identity.log", "w+", encoding="utf8")
        cls.port = free_port()
        cls.identity_port = free_port()
        cls.identity = subprocess.Popen([str(BASE / "server" / "identidad"), str(cls.identity_port)],
                                        stdout=cls.id_out, stderr=subprocess.STDOUT)
        env = os.environ.copy()
        env.update(SMDP_IDENTITY_HOST="localhost", SMDP_IDENTITY_PORT=str(cls.identity_port),
                   SMDP_DROP_FIRST_ACK="1")
        cls.server = subprocess.Popen([str(BASE / "server" / "servidor"), str(cls.port), str(cls.log)],
                                      stdout=cls.out, stderr=subprocess.STDOUT, env=env)
        for attempt in range(80):
            try:
                with socket.create_connection(("localhost", cls.port), timeout=.2):
                    break
            except OSError:
                time.sleep(.05)
        else:
            raise RuntimeError("No inició servidor de prueba")

    @classmethod
    def tearDownClass(cls):
        cls.server.terminate(); cls.identity.terminate()
        try: cls.server.wait(timeout=3)
        except subprocess.TimeoutExpired: cls.server.kill()
        try: cls.identity.wait(timeout=3)
        except subprocess.TimeoutExpired: cls.identity.kill()
        cls.out.close(); cls.id_out.close(); cls.tmp.cleanup()

    def request(self, kind, origin, seq, token, payload, sock=None, reader=None):
        owned = sock is None
        if owned:
            sock = socket.create_connection(("localhost", self.port), timeout=5)
            reader = sock.makefile("rb")
        try:
            sock.sendall(message(kind, origin, seq, token, payload))
            return recv_frame(reader)
        finally:
            if owned:
                reader.close(); sock.close()

    def register(self, name):
        reg = self.request("REG", name, 1, "-", {"device_type": "sensor", "location": "sala1"})
        self.assertEqual(reg["kind"], "REG_OK")
        self.assertEqual(len(reg["token"]), 32)
        return reg["token"]

    def login(self, name="juan", pwd="1234"):
        return self.request("AUTH", name, 1, "-", {"user": name, "pass": pwd})

    def udp(self, raw, timeout=0.25):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(timeout);s.sendto(raw,("localhost", self.port))
            try:
                return parse(s.recvfrom(4097)[0])
            except socket.timeout:
                return None

    def test_01_multi_node_history_and_out_of_order(self):
        for name in ("nodo-01", "nodo-02"):
            token=self.register(name)
            for seq in range(2, 9):
                raw=message("STATUS",name,seq,token,
                            {"cpu":seq*10,"temp":40.5,"battery":80,"state":"ok"})
                self.assertIsNone(self.udp(raw,.05))  # STATUS intencionalmente sin ACK
            # datagrama antiguo no puede reemplazar el estado reciente
            self.udp(message("STATUS",name,3,token,
                        {"cpu":3,"temp":0,"battery":1,"state":"stale"}),.05)
            auth=self.login()
            resp=self.request("QUERY","juan",2,auth["token"],
                              {"resource":"history","node":name,"limit":5})
            self.assertEqual(resp["kind"],"QUERY_RESP")
            hist=resp["data"]["history"]
            self.assertEqual(len(hist),5)
            self.assertEqual([r["seq"] for r in hist],[4,5,6,7,8])
            status=self.request("QUERY","juan",3,auth["token"],
                                {"resource":"status","node":name})
            self.assertEqual(status["data"]["cpu"],80)

    def test_02_event_lost_ack_dedup_and_events_query(self):
        name="nodo-events";token=self.register(name)
        event=message("EVENT",name,14,token,{"event":"threshold","value":95,"threshold":90})
        with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as sock:
            sock.settimeout(.3)
            sock.sendto(event,("localhost",self.port))
            with self.assertRaises(socket.timeout):
                sock.recvfrom(4097)  # pérdida simulada del primer ACK
            sock.sendto(event,("localhost",self.port))
            answer=parse(sock.recvfrom(4097)[0])
            self.assertEqual(answer["kind"],"ACK")
            self.assertEqual(answer["seq"],14)
            sock.sendto(event,("localhost",self.port))
            self.assertTrue(parse(sock.recvfrom(4097)[0])["data"].get("duplicate"))
        auth=self.login()
        result=self.request("QUERY","juan",2,auth["token"],
                            {"node":name,"resource":"events","limit":5})
        self.assertEqual(len(result["data"]["events"]),1)
        self.assertEqual(result["data"]["events"][0]["seq"],14)

    def test_03_auth_roles_and_unknown_node(self):
        self.assertEqual(self.login("juan","incorrecta")["kind"],"AUTH_ERR")
        david=self.login("david_rodriguez_espinosa")
        self.assertEqual(david["kind"],"AUTH_OK")
        self.assertEqual(david["data"]["profile"],"ADMIN")
        visor=self.login("maria")
        self.assertEqual(visor["kind"],"AUTH_OK")
        forbidden=self.request("QUERY","maria",2,visor["token"],{"resource":"nodes"})
        self.assertEqual(forbidden["data"]["code"],"UNAUTHORIZED")
        admin=self.login()
        visible=self.request("QUERY","juan",2,admin["token"],{"resource":"nodes"})
        self.assertEqual(visible["kind"],"QUERY_RESP")
        unknown=self.request("QUERY","juan",3,admin["token"],
                             {"resource":"history","node":"no-existe"})
        self.assertEqual(unknown["data"]["code"],"NOT_FOUND")
        unauth=self.request("QUERY","juan",4,"-",{"resource":"nodes"})
        self.assertEqual(unauth["data"]["code"],"UNAUTHORIZED")

    def test_04_malformed_unknown_version_and_tcp_framing(self):
        with socket.create_connection(("localhost",self.port),timeout=5) as s:
            s.settimeout(4)
            with s.makefile("rb") as reader:
                # Envía REG fragmentado para reproducir recv parciales.
                reg=message("REG","framed-node",1,"-",{"device_type":"sensor","location":"sala1"})
                s.sendall(reg[:8]);time.sleep(.05);s.sendall(reg[8:])
                self.assertEqual(recv_frame(reader)["kind"],"REG_OK")
                # Dos peticiones en una sola escritura TCP => dos respuestas.
                s.sendall(message("BOGUS","framed-node",2,"-",{})+
                          b'SMDP/9.9|AUTH|juan|3|1|-|{"user":"juan","pass":"1234"}\n')
                self.assertEqual(recv_frame(reader)["data"]["code"],"UNKNOWN_TYPE")
                self.assertEqual(recv_frame(reader)["data"]["code"],"UNSUPPORTED_VERSION")
                s.sendall(b'SMDP/1.0|AUTH|juan|4|1|-|{"user":"juan","pass":}\n')
                self.assertEqual(recv_frame(reader)["data"]["code"],"MALFORMED_MESSAGE")
                s.sendall(b'A'*4100+b'\n')
                self.assertEqual(recv_frame(reader)["data"]["code"],"MALFORMED_MESSAGE")
                s.sendall(b'A'*4096+b'\n'+message("BOGUS","framed-node",6,"-",{}))
                self.assertEqual(recv_frame(reader)["data"]["code"],"MALFORMED_MESSAGE")
                self.assertEqual(recv_frame(reader)["data"]["code"],"UNKNOWN_TYPE")
                # servidor debe continuar tras errores.
                s.sendall(message("AUTH","juan",7,"-",{"user":"juan","pass":"1234"}))
                self.assertEqual(recv_frame(reader)["kind"],"AUTH_OK")

    def test_05_udp_unknown_node_and_invalid_metric(self):
        response=self.udp(message("STATUS","unknown",2,"-",{
            "cpu":5,"temp":30,"battery":90,"state":"ok"}))
        self.assertEqual(response["data"]["code"],"NODE_NOT_REGISTERED")
        token=self.register("invalid-metrics")
        response=self.udp(message("STATUS","invalid-metrics",2,token,{
            "cpu":-99,"temp":30,"battery":90,"state":"ok"}))
        self.assertEqual(response["data"]["code"],"MALFORMED_MESSAGE")

    def test_06_disconnection_and_recovery(self):
        with socket.create_connection(("localhost",self.port),timeout=5) as sock:
            sock.sendall(b"SMDP/1.0|AUTH|juan|")
            # Corte repentino en mitad de una petición.
        self.assertEqual(self.login()["kind"],"AUTH_OK")

    def test_10_identity_incomplete_request_times_out(self):
        with socket.create_connection(("localhost",self.identity_port),timeout=4) as sock:
            sock.settimeout(4)
            sock.sendall(b"IDENT/1|juan|")
            response=sock.makefile("rb").readline()
        self.assertEqual(response,b"DENIED\n")

    def test_07_invalid_udp_datagram(self):
        response=self.udp(b"SMDP/1.0|STATUS|nodo|abc|1|-|{}\n")
        self.assertEqual(response["data"]["code"],"MALFORMED_MESSAGE")
        response=self.udp(b"SMDP/1.0|STATUS|nodo|1|1|-|{}" + b"A"*5000 + b"\n")
        self.assertEqual(response["data"]["code"],"MALFORMED_MESSAGE")
        response=self.udp(b"SMDP/1.0|STATUS|nodo|1|1|-|{\"state\":\""+b"\xff"+b"\"}\n")
        self.assertEqual(response["data"]["code"],"MALFORMED_MESSAGE")

    def test_08_admin_pagination(self):
        auth=self.login()
        first=self.request("QUERY","juan",2,auth["token"],{"resource":"nodes","limit":1,"offset":0})
        self.assertEqual(first["kind"],"QUERY_RESP")
        self.assertEqual(first["data"]["shown"],1)
        self.assertGreaterEqual(first["data"]["total"],1)

    def test_09_two_nodes_send_concurrently(self):
        def node_run(node_name):
            token=self.register(node_name)
            with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as sock:
                for seq in range(2,7):
                    sock.sendto(message("STATUS",node_name,seq,token,{
                        "cpu":seq,"temp":35,"battery":99,"state":"ok"}),
                        ("localhost",self.port))
            return node_name
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(set(pool.map(node_run,("sim-a","sim-b"))),{"sim-a","sim-b"})
        # Esperar que los workers UDP consuman ambos flujos antes de consultar.
        auth=self.login()
        for _ in range(100):
            a=self.request("QUERY","juan",2,auth["token"],
                           {"resource":"history","node":"sim-a"})
            b=self.request("QUERY","juan",3,auth["token"],
                           {"resource":"history","node":"sim-b"})
            if len(a["data"]["history"])==5 and len(b["data"]["history"])==5:
                break
            time.sleep(.01)
        self.assertEqual(len(a["data"]["history"]),5)
        self.assertEqual(len(b["data"]["history"]),5)

    def test_06_concurrent_admins_and_logs(self):
        self.register("concurrent")
        def work(i):
            auth=self.login()
            return self.request("QUERY","juan",i+2,auth["token"],
                                {"resource":"status","node":"concurrent"})["kind"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(list(pool.map(work,range(8))),["QUERY_RESP"]*8)
        content=self.log.read_text(encoding="utf8")
        for required in ("transport=TCP", "transport=UDP", "direction=in", "direction=out", "peer="):
            self.assertIn(required,content)
        self.assertNotIn("pass=",content)
        self.assertIn("SIMULATED_LOSS",content)


if __name__=="__main__":
    unittest.main(verbosity=2)
