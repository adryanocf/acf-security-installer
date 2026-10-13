#!/usr/bin/env python3
import sys
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

CONFIG = Path(__file__).with_name("connector.env")
ALLOWED_EVENTS = {"IsMotion", "IsPeople", "IsPet", "IsCar"}

def load_config():
    values = {}
    for line in CONFIG.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values

def main():
    if len(sys.argv) < 3:
        print("Uso: connector-handler.py camera_id evento/AAAAMMDD-HHMMSS [...]")
        return 2

    cfg = load_config()
    base_url = cfg["ACF_SECURITY_URL"].rstrip("/")
    installation_id = cfg["ACF_INSTALLATION_ID"]
    secret = cfg["ACF_INSTALLATION_SECRET"]
    camera_id = cfg["ACF_CAMERA_ID"]

    for argument in sys.argv[2:]:
        event_type, separator, event_time = argument.partition("/")
        if not separator or event_type not in ALLOWED_EVENTS:
            continue

        try:
            local_time = datetime.strptime(event_time, "%Y%m%d-%H%M%S")
            occurred_at = local_time.astimezone(timezone.utc).isoformat(
                timespec="seconds"
            ).replace("+00:00", "Z")
        except ValueError:
            print(f"Horário inválido para evento {event_type}")
            continue

        event_key = f"{sys.argv[1]}|{event_type}|{event_time}"
        event_id = "onvif-" + hashlib.sha256(
            event_key.encode()
        ).hexdigest()[:40]

        payload = {
            "camera_id": camera_id,
            "event_id": event_id,
            "type": event_type,
            "state": True,
            "occurred_at": occurred_at,
            "camera_ref": sys.argv[1],
            "description": "Detecção recebida via ONVIF"
        }

        request = Request(
            base_url + "/api/public/connector-events",
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "curl/8.0 ACF-SECURITY-Connector",
                "X-ACF-Installation": installation_id,
                "Authorization": "Bearer " + secret
            },
            method="POST"
        )

        try:
            with urlopen(request, timeout=15) as response:
                print(f"{event_type}: HTTP {response.status}")
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            print(f"{event_type}: HTTP {error.code} — {detail[:500]}")
        except (URLError, TimeoutError) as error:
            print(f"{event_type}: falha de conexão ({type(error).__name__})")

    return 0

if __name__ == "__main__":
    sys.exit(main())
