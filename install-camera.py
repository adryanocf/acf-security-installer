#!/usr/bin/env python3
import getpass
import ipaddress
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path("/opt/acf-security")
IMAGE = "acf-security-onvif:local"
HANDLER = ROOT / "connector-handler.py"
HEARTBEAT = ROOT / "heartbeat.py"


def ask(label, default=None, secret=False):
    prompt = f"{label}"
    if default:
        prompt += f" [{default}]"
    prompt += ": "
    value = getpass.getpass(prompt) if secret else input(prompt)
    value = value.strip()
    return value or (default or "")


def install_heartbeat_cron(base):
    script = str(base / "heartbeat.py")
    log_path = str(base / "logs" / "heartbeat.log")

    current = subprocess.run(
        ["crontab", "-l"], text=True, capture_output=True
    )
    if current.returncode not in (0, 1):
        raise RuntimeError("Não foi possível ler o crontab atual.")

    lines = current.stdout.splitlines() if current.returncode == 0 else []
    if any(script in line for line in lines):
        return False

    entry = f"*/5 * * * * /usr/bin/python3 {script} >> {log_path} 2>&1"
    updated = subprocess.run(
        ["crontab", "-"],
        input=chr(10).join(lines + [entry]) + chr(10),
        text=True, capture_output=True
    )
    if updated.returncode:
        raise RuntimeError("Não foi possível gravar o crontab.")
    return True


def main():
    if os.geteuid() != 0:
        sys.exit("Execute como root.")

    if not HANDLER.is_file():
        sys.exit(f"Conector compartilhado não encontrado: {HANDLER}")
    if not HEARTBEAT.is_file():
        sys.exit(f"Script de heartbeat não encontrado: {HEARTBEAT}")

    image_dir = ROOT / "image"
    if not (image_dir / "Dockerfile").is_file() or not (image_dir / "onvifeye.py").is_file():
        sys.exit(f"Arquivos para construir a imagem não encontrados em: {image_dir}")

    if subprocess.run(
        ["docker", "image", "inspect", IMAGE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    ).returncode:
        print(f"Imagem {IMAGE} ausente. Construindo a partir de {image_dir}...")
        build = subprocess.run(["docker", "build", "-t", IMAGE, str(image_dir)])
        if build.returncode:
            sys.exit("Falha ao construir a imagem Docker. Nenhuma câmera nova foi instalada.")

    print("\n=== Instalador de câmera ACF SECURITY ===")
    slug = ask("Identificador local (letras, números e hífen)")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-]{1,40}", slug):
        sys.exit("Identificador inválido.")

    base = ROOT / "clients" / slug
    container = f"acf-security-{slug}"

    if base.exists():
        sys.exit(f"A pasta {base} já existe. Nada foi sobrescrito.")
    if subprocess.run(
        ["docker", "container", "inspect", container],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    ).returncode == 0:
        sys.exit(f"O contêiner {container} já existe.")

    model = ask("Modelo da câmera")
    ip = ask("IP da câmera")
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        sys.exit("Endereço IP inválido.")

    port = ask("Porta ONVIF", "2020")
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        sys.exit("Porta inválida.")

    username = ask("Usuário ONVIF da câmera")
    password = ask("Senha ONVIF da câmera", secret=True)
    if not username or not password:
        sys.exit("Usuário e senha da câmera são obrigatórios.")

    print("\nCredenciais da instalação ACF SECURITY:")
    api_url = ask("URL base da plataforma", "https://acfsecurity.lovable.app").rstrip("/")
    parsed = urlparse(api_url)
    if parsed.scheme != "https" or not parsed.netloc:
        sys.exit("Informe uma URL HTTPS válida.")

    installation_id = ask("ACF_INSTALLATION_ID")
    installation_secret = ask("ACF_INSTALLATION_SECRET", secret=True)
    acf_camera_id = ask("ACF_CAMERA_ID (identificador interno da câmera na plataforma)")
    if not installation_id or not installation_secret or not acf_camera_id:
        sys.exit("Todos os campos de autenticação são obrigatórios.")

    events = ["IsMotion", "IsPeople", "IsPet", "IsCar"]
    base.mkdir(mode=0o750, parents=True)
    (base / "config").mkdir(mode=0o750)
    (base / "output").mkdir(mode=0o750)
    (base / "logs").mkdir(mode=0o750)

    camera_config = {
        "camera_id": slug,
        "camera_model": model,
        "camera_ip_addr": ip,
        "camera_onvif_port": str(port),
        "camera_username": username,
        "camera_password": password,
        "camera_target_events": events,
        "camera_save_folder": str(base / "output"),
        "camera_event_exec": str(base / "connector-handler.py"),
        "camera_event_exec_delay_seconds": 2
    }

    config_path = base / "config" / (slug + ".conf")
    config_path.write_text(
        json.dumps(camera_config, indent=2, ensure_ascii=False) + "\n"
    )
    config_path.chmod(0o600)

    secret_path = base / "connector.env"
    secret_path.write_text(
        f"ACF_SECURITY_URL={api_url}\n"
        f"ACF_INSTALLATION_ID={installation_id}\n"
        f"ACF_INSTALLATION_SECRET={installation_secret}\n"
        f"ACF_CAMERA_ID={acf_camera_id}\n"
    )
    secret_path.chmod(0o600)

    handler_path = base / "connector-handler.py"
    handler_path.write_bytes(HANDLER.read_bytes())
    handler_path.chmod(0o700)

    heartbeat_path = base / "heartbeat.py"
    heartbeat_path.write_bytes(HEARTBEAT.read_bytes())
    heartbeat_path.chmod(0o700)

    # Evita manter as senhas em variáveis do programa após gravar os arquivos.
    camera_config["camera_password"] = ""
    password = ""
    installation_secret = ""

    command = [
        "docker", "run", "-d",
        "--name", container,
        "--restart", "unless-stopped",
        "--network", "host",
        "--log-opt", "max-size=10m",
        "--log-opt", "max-file=3",
        "-v", f"{base}:{base}",
        "-v", f"{base / 'config'}:/root/.config/onvifeye/camera_conf:ro",
        IMAGE
    ]

    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode:
        print("Falha ao iniciar o contêiner. Iniciando limpeza desta instalação.")

        # Remove somente um contêiner com o nome reservado por esta instalação.
        inspect = subprocess.run(
            ["docker", "container", "inspect", container],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        if inspect.returncode == 0:
            subprocess.run(
                ["docker", "rm", "-f", container],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

        import shutil
        shutil.rmtree(base, ignore_errors=True)

        print(result.stderr.strip())
        print("Arquivos desta instalação removidos; as demais instalações foram preservadas.")
        sys.exit(result.returncode)

    try:
        if install_heartbeat_cron(base):
            print("Heartbeat agendado a cada 5 minutos.")
        else:
            print("Heartbeat já agendado; tarefa duplicada evitada.")
    except Exception as exc:
        print(f"AVISO: não foi possível agendar o heartbeat: {exc}")

    print("\nInstalação criada.")
    print(f"Contêiner: {container}")
    print(f"Pasta: {base}")
    print("Reinício automático: unless-stopped")
    print("Confira os logs antes de considerar a câmera operacional:")
    print(f"docker logs --tail 40 {container}")


if __name__ == "__main__":
    main()
