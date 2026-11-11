"""Pruebas unitarias para la codificacion y decodificacion de 3 bytes (24 bits)."""
import unittest

from codec import (
    DIRECCIONES,
    bytes_to_json,
    decode_payload,
    encode_payload,
    inspeccionar_bits,
    json_to_bytes,
)


class TestCodec(unittest.TestCase):
    def test_ejemplo_guia(self):
        original = {"temperatura": 56.32, "humedad": 51, "direccion_viento": "SO"}
        encoded = encode_payload(original)
        self.assertEqual(len(encoded), 3)

        decoded = decode_payload(encoded)
        self.assertEqual(decoded["temperatura"], 56.32)
        self.assertEqual(decoded["humedad"], 51)
        self.assertEqual(decoded["direccion_viento"], "SO")

    def test_limites_minimos(self):
        original = {"temperatura": 0.0, "humedad": 0, "direccion_viento": "N"}
        encoded = encode_payload(original)
        self.assertEqual(len(encoded), 3)
        self.assertEqual(encoded, b"\x00\x00\x00")

        decoded = decode_payload(encoded)
        self.assertEqual(decoded["temperatura"], 0.0)
        self.assertEqual(decoded["humedad"], 0)
        self.assertEqual(decoded["direccion_viento"], "N")

    def test_limites_maximos(self):
        original = {"temperatura": 110.0, "humedad": 100, "direccion_viento": "NO"}
        encoded = encode_payload(original)
        self.assertEqual(len(encoded), 3)

        decoded = decode_payload(encoded)
        self.assertEqual(decoded["temperatura"], 110.0)
        self.assertEqual(decoded["humedad"], 100)
        self.assertEqual(decoded["direccion_viento"], "NO")

    def test_todas_las_direcciones(self):
        for d in DIRECCIONES:
            data = {"temperatura": 25.50, "humedad": 50, "direccion_viento": d}
            encoded = encode_payload(data)
            self.assertEqual(len(encoded), 3)
            decoded = decode_payload(encoded)
            self.assertEqual(decoded["direccion_viento"], d)

    def test_json_to_bytes_y_bytes_to_json(self):
        json_in = '{"temperatura": 78.56, "humedad": 65, "direccion_viento": "E"}'
        b = json_to_bytes(json_in)
        self.assertEqual(len(b), 3)
        json_out = bytes_to_json(b)
        self.assertIn('"temperatura": 78.56', json_out)
        self.assertIn('"humedad": 65', json_out)
        self.assertIn('"direccion_viento": "E"', json_out)

    def test_inspeccionar_bits(self):
        b = encode_payload({"temperatura": 55.0, "humedad": 55, "direccion_viento": "S"})
        txt = inspeccionar_bits(b)
        self.assertIn("Temp: 14b", txt)
        self.assertIn("Hum: 7b", txt)
        self.assertIn("Viento: 3b", txt)
        self.assertIn("= 3 bytes", txt)


if __name__ == "__main__":
    unittest.main()
