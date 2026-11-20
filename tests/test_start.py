import sys
from unittest.mock import Mock

from scripts import start


def test_launcher_builds_the_shared_image_once(monkeypatch, tmp_path):
    (tmp_path / ".env").write_text("POSTGRES_PASSWORD=test-only\n", encoding="utf-8")
    monkeypatch.setattr(start, "ROOT", tmp_path)
    monkeypatch.setattr(start.shutil, "which", lambda _: "docker")
    monkeypatch.setattr(sys, "argv", ["start.py"])
    run = Mock()
    monkeypatch.setattr(start.subprocess, "run", run)

    start.main()

    compose = ["docker", "compose", "-f", "docker-compose.yml"]
    assert [call.args[0] for call in run.call_args_list] == [
        compose + ["build", "dashboard"],
        compose + ["up", "--no-build", "-d"],
    ]
