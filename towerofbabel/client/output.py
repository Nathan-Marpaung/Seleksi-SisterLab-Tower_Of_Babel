from __future__ import annotations

import json
from typing import Any


def emit(payload: Any, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        if isinstance(payload, dict) and "status" in payload:
            print(f"{payload.get('status').upper()}: {payload.get('operation', '')} via {payload.get('service_id')}")
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(json.dumps(payload, indent=2, sort_keys=True))

