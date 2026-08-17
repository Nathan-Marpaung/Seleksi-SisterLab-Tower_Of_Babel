from __future__ import annotations

from typing import Any

import httpx

from client.models import ExecuteRequest, validate_gateway_response


class GatewayUnavailable(RuntimeError):
    pass


class GatewayInvalidResponse(RuntimeError):
    pass


class HttpGatewayConnector:
    def __init__(self, base_url: str, timeout_ms: int = 2000) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_ms / 1000

    async def execute(self, request: ExecuteRequest) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(f"{self.base_url}/execute", json=request.as_json())
                response.raise_for_status()
                payload = response.json()
        except httpx.ConnectError as exc:
            raise GatewayUnavailable(f"Gateway unavailable at configured endpoint: {self.base_url}") from exc
        except httpx.TimeoutException as exc:
            raise GatewayUnavailable(f"Gateway timed out at configured endpoint: {self.base_url}") from exc
        except (httpx.HTTPError, ValueError) as exc:
            raise GatewayInvalidResponse(f"Invalid gateway response: {exc}") from exc
        try:
            validate_gateway_response(payload)
        except ValueError as exc:
            raise GatewayInvalidResponse(str(exc)) from exc
        return payload

    async def services(self) -> dict[str, Any]:
        return await self._get("/services")

    async def status(self) -> dict[str, Any]:
        return await self._get("/status")

    async def _get(self, path: str) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.base_url}{path}")
                response.raise_for_status()
                return response.json()
        except httpx.ConnectError as exc:
            raise GatewayUnavailable(f"Gateway unavailable at configured endpoint: {self.base_url}") from exc
        except httpx.TimeoutException as exc:
            raise GatewayUnavailable(f"Gateway timed out at configured endpoint: {self.base_url}") from exc
        except (httpx.HTTPError, ValueError) as exc:
            raise GatewayInvalidResponse(f"Invalid gateway response: {exc}") from exc

