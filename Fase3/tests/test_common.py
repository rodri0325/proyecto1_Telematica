"""Pruebas unitarias de recuperación de nombres y enmarcado del cliente."""
import io
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clientes"))
import common


class CommonTests(unittest.TestCase):
    def test_partial_tcp_frame(self):
        obj=common.recv_frame(io.BytesIO(common.message("QUERY_RESP","server",4,"-",{"x":123})))
        self.assertEqual(obj["data"]["x"],123)

    def test_invalid_frame_and_utf8(self):
        for value in (b"", b"hello\n", b"SMDP/1.0|QUERY_RESP|server|1|2|-|{}",
                      b"SMDP/1.0|QUERY_RESP|server|x|2|-|{}\n",b"X\n"*2050):
            with self.assertRaises(common.ProtocolError):
                common.parse(value)

    def test_dns_failure_retries(self):
        expected=[(socket.AF_INET,socket.SOCK_STREAM,6,"",(socket.gethostbyname("localhost"),6000))]
        with patch.object(common.socket,"getaddrinfo",side_effect=[socket.gaierror("fallo DNS"),expected]) as lookup:
            with patch.object(common.time,"sleep") as wait:
                self.assertEqual(common.resolve("servidor.ejemplo.local",6000,socket.SOCK_STREAM),expected)
                self.assertEqual(lookup.call_count,2)
                wait.assert_called_once_with(1)

if __name__=="__main__":
    unittest.main()
