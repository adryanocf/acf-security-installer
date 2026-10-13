#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

CONFIG = Path(__file__).with_name("connector.env")

def main():
    cfg = {}
    for line in CONFIG.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            cfg[key.strip()] = value.strip().strip("'\"")

    required = (
        "ACF_SECURITY_URL",
        "ACF_INSTALLATION_ID",
        "ACF_INSTALLATION_SECRET",
        "ACF_CAMERA_ID",
    )
    missing = [key for key in required if not cfg.get(key)]
    if missing:
        print("Configuração incompleta: " + ", ".join(missing))
        return 1

    payload = {
        "type": "Heartbeat",
        "camera_id": cfg["ACF_CAMERA_ID"],
        "sent_at": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z"),
    }

    request = Request(
        cfg["ACF_SECURITY_URL"].rstrip("/")
        + "/api/public/connector-events",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "curl/8.0 ACF-SECURITY-Connector",
            "X-ACF-Installation": cfg["ACF_INSTALLATION_ID"],
            "Authorization": "Bearer " + cfg["ACF_INSTALLATION_SECRET"],
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=20) as response:
            result = json.loads(response.read().decode())
            if response.status == 200 and result.get("status") == "heartbeat_ok":
                print("Heartbeat confirmado pelo servidor:", result.get("received_at"))
                return 0
            print("Resposta inesperada:", response.status, result)
            return 1
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        print("Heartbeat rejeitado: HTTP", error.code)
        print("Resposta da API:", detail[:1000])
        if error.code in (401, 403):
            print("Não agende novos heartbeats até resolver a autorização.")
        return 2
    except (URLError, TimeoutError) as error:
        print("Falha de comunicação:", type(error).__name__)
        return 1
    except (ValueError, OSError, KeyError) as error:
        print("Erro de configuração ou resposta:", type(error).__name__)
        return 1

if __name__ == "__main__":
    sys.exit(main())
