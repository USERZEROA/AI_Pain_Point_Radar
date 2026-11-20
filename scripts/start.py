#!/usr/bin/env python3
"""One-command startup. Creates local secrets only when no .env exists."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("live", "replay"), default=None)
    parser.add_argument("--public-domain", help="Real domain resolving to this host; enables HTTPS gateway")
    parser.add_argument("--no-start", action="store_true", help="Prepare .env only, useful for CI")
    args = parser.parse_args()
    if args.public_domain:
        domain = args.public_domain.strip()
        if not domain or any(c in domain for c in "\n\r /\\$") or "." not in domain:
            parser.error("--public-domain must be a hostname, not a URL")
        args.public_domain = domain
    path = ROOT / ".env"
    if not path.exists():
        text = (ROOT / ".env.example").read_text(encoding="utf-8")
        text = text.replace("REPLACE_WITH_A_RANDOM_PASSWORD", secrets.token_hex(24))
        if args.mode:
            text = text.replace("STREAM_MODE=replay", f"STREAM_MODE={args.mode}")
        if args.public_domain:
            text = text.replace("PUBLIC_DOMAIN=localhost", f"PUBLIC_DOMAIN={args.public_domain}")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(text)
        print("Created private .env with a generated database password.")
    elif "POSTGRES_PASSWORD=REPLACE_WITH_A_RANDOM_PASSWORD" in path.read_text(encoding="utf-8"):
        parser.error("Replace POSTGRES_PASSWORD in .env; the example value is not a real password")
    if args.no_start:
        return
    if not shutil.which("docker"):
        parser.error("Docker is required. Install Docker Desktop or Docker Engine with Compose v2, then rerun.")
    env = os.environ.copy()
    if args.mode:
        env["STREAM_MODE"] = args.mode
    if args.public_domain:
        env["PUBLIC_DOMAIN"] = args.public_domain
    command = ["docker", "compose", "-f", "docker-compose.yml"]
    if args.public_domain:
        command += ["-f", "docker-compose.public.yml"]
    # The producer, consumers and dashboard share the dashboard service's image.
    subprocess.run(command + ["build", "dashboard"], cwd=ROOT, env=env, check=True)
    subprocess.run(command + ["up", "--no-build", "-d"], cwd=ROOT, env=env, check=True)
    print("Dashboard: " + (f"https://{args.public_domain}" if args.public_domain else "http://localhost:8501"))
    print("Initial genuine-data collection may take a few minutes. See: docker compose logs -f producer")


if __name__ == "__main__":
    main()
