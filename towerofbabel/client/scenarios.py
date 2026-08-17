from __future__ import annotations

import asyncio
from typing import Any

from client.gateway_connector import HttpGatewayConnector
from client.models import ExecuteRequest


async def run_scenario(name: str, connector: HttpGatewayConnector) -> dict[str, Any]:
    if name == "concurrency":
        tasks = [
            connector.execute(ExecuteRequest(f"client-concurrent-{i:03d}", "echo", {"value": f"v{i}"}, {"timeout_ms": 2000}))
            for i in range(20)
        ]
        return {"scenario": name, "responses": await asyncio.gather(*tasks)}
    if name == "full-demo":
        responses = []
        for op, args in [("echo", {"value": "hello"}), ("uppercase", {"value": "babel"}), ("sum", {"values": [1, 2, 3]}), ("reverse", {"value": "babel"})]:
            responses.append(await connector.execute(ExecuteRequest(f"client-demo-{op}", op, args, {"timeout_ms": 2000})))
        return {"scenario": name, "responses": responses}
    return {"scenario": name, "response": await connector.execute(ExecuteRequest(f"client-{name}", "echo", {"value": name}, {"timeout_ms": 2000}))}

