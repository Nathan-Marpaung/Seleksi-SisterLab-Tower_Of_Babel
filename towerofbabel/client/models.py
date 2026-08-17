from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExecuteRequest:
    request_id: str
    operation: str
    arguments: dict[str, Any]
    options: dict[str, Any]

    def as_json(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "operation": self.operation,
            "arguments": self.arguments,
            "options": self.options,
        }


REQUIRED_RESPONSE_KEYS = {"request_id", "status", "service_id", "operation", "result", "error"}


def validate_gateway_response(payload: dict[str, Any]) -> None:
    missing = REQUIRED_RESPONSE_KEYS - set(payload)
    if missing:
        raise ValueError(f"Invalid gateway response; missing keys: {sorted(missing)}")
    if payload["status"] not in {"success", "error"}:
        raise ValueError("Invalid gateway response; status must be success or error.")

