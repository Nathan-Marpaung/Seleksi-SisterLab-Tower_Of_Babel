from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from client.gateway_connector import GatewayInvalidResponse, GatewayUnavailable, HttpGatewayConnector
from client.models import ExecuteRequest
from client.output import emit
from client.scenarios import run_scenario


def request_id() -> str:
    return f"client-{int(time.time() * 1000)}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="babel-client")
    parser.add_argument("--gateway-url", default=os.getenv("BABEL_GATEWAY_URL", "http://localhost:8080"))
    parser.add_argument("--timeout-ms", type=int, default=int(os.getenv("BABEL_DEFAULT_TIMEOUT_MS", "2000")))
    parser.add_argument("--json", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    ex = sub.add_parser("execute")
    ex.add_argument("operation")
    ex.add_argument("--value")
    ex.add_argument("--values", nargs="*", type=float)
    ex.add_argument("--request-id")
    ex.add_argument("--preferred-service")
    sub.add_parser("services")
    sub.add_parser("status")
    sc = sub.add_parser("scenario")
    sc.add_argument("name")
    raw = sub.add_parser("raw")
    raw.add_argument("request_file")
    return parser.parse_args()


def build_arguments(args: argparse.Namespace) -> dict[str, Any]:
    if args.operation == "sum":
        return {"values": args.values or []}
    if args.operation in {"echo", "uppercase", "reverse"}:
        return {"value": args.value or ""}
    return {}


async def async_main() -> int:
    args = parse_args()
    connector = HttpGatewayConnector(args.gateway_url, args.timeout_ms)
    try:
        if args.command == "services":
            emit(await connector.services(), args.json)
        elif args.command == "status":
            emit(await connector.status(), args.json)
        elif args.command == "scenario":
            emit(await run_scenario(args.name, connector), args.json)
        elif args.command == "raw":
            payload = json.loads(Path(args.request_file).read_text(encoding="utf-8"))
            req = ExecuteRequest(payload["request_id"], payload["operation"], payload.get("arguments", {}), payload.get("options", {}))
            emit(await connector.execute(req), args.json)
        else:
            req = ExecuteRequest(
                args.request_id or request_id(),
                args.operation,
                build_arguments(args),
                {"preferred_service": args.preferred_service, "timeout_ms": args.timeout_ms},
            )
            emit(await connector.execute(req), args.json)
        return 0
    except GatewayUnavailable as exc:
        print(str(exc))
        return 2
    except GatewayInvalidResponse as exc:
        print(str(exc))
        return 3


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
