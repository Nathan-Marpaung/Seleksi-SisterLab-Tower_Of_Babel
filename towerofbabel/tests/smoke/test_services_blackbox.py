from __future__ import annotations

import json
import socket
import struct
import urllib.request
import zlib

import pytest


def test_service_a_http_contract() -> None:
    body = json.dumps({"operation_name": "uppercase", "operation_arguments": {"value": "babel"}}).encode()
    request = urllib.request.Request(
        "http://localhost:8101/v1/execute",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "X-Protocol-Version": "1", "X-Request-ID": "smoke-a"},
    )
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            payload = json.loads(response.read())
    except OSError:
        pytest.skip("Service A is not running")
    assert payload["operation_result"] == {"value": "BABEL"}


def test_service_b_tcp_contract() -> None:
    payload = json.dumps({"opCode": "SUM", "args": {"numberList": [1, 2, 3]}}).encode()
    frame = b"\xBA\xBE" + bytes([1, 0]) + struct.pack("!IQ", len(payload), 42) + payload
    try:
        sock = socket.create_connection(("localhost", 8201), timeout=2)
    except OSError:
        pytest.skip("Service B is not running")
    with sock:
        sock.sendall(frame)
        header = sock.recv(16)
        magic, version, _flags, length, request_id = struct.unpack("!2sBBIQ", header)
        body = sock.recv(length)
    assert magic == b"\xBA\xBE"
    assert version == 1
    assert request_id == 42
    assert json.loads(body)["resultData"]["numericResult"] == 6


def test_service_c_udp_contract() -> None:
    body = json.dumps({"values": [7, 8]}).encode()
    prefix = b"\xC0\xDE" + bytes([1, 1]) + struct.pack("!IQBBH", 1, 333, 2, 0, len(body)) + body
    packet = prefix + struct.pack("!I", zlib.crc32(prefix) & 0xFFFFFFFF)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)
    try:
        sock.sendto(packet, ("localhost", 8301))
        data, _addr = sock.recvfrom(8192)
    except OSError:
        pytest.skip("Service C is not running")
    finally:
        sock.close()
    assert data[:2] == b"\xC0\xDE"
    assert zlib.crc32(data[:-4]) & 0xFFFFFFFF == struct.unpack("!I", data[-4:])[0]
    length = struct.unpack("!H", data[18:20])[0]
    payload = json.loads(data[20 : 20 + length])
    assert payload["result"]["value"] == 15

