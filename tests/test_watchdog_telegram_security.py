"""Offline checks for the watchdog's credential-bearing Telegram request."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("token,called", [
    ("12345:synthetic_fake_token", True),
    ('12345:bad"config', False),
])
def test_watchdog_request_keeps_token_out_of_process_args_and_output(
    tmp_path: Path, token: str, called: bool,
) -> None:
    source = (Path(__file__).resolve().parents[1] / "scripts/watchdog.sh").read_text()
    match = re.search(r"(?ms)^send_telegram\(\) \{\n.*?^\}", source)
    assert match

    output = tmp_path / "capture.json"
    curl = tmp_path / "curl"
    curl.write_text(
        "#!/usr/bin/python3\n"
        "import json, os, pathlib, sys\n"
        "data = sys.stdin.read()\n"
        "pathlib.Path(os.environ['CAPTURE_PATH']).write_text(json.dumps({\n"
        "  'argv': sys.argv[1:], 'config': data,\n"
        "  'token_in_env': 'TG_BOT_TOKEN' in os.environ,\n"
        "}))\n"
        "print('stdout synthetic_fake_token')\n"
        "print('stderr synthetic_fake_token', file=sys.stderr)\n"
        "sys.exit(7)\n"
    )
    curl.chmod(0o700)
    env = {
        "PATH": f"{tmp_path}:/usr/bin:/bin",
        "TG_BOT_TOKEN": token,
        "TG_CHAT_ID": "100",
        "TG_TOPIC_ID": "200",
        "CAPTURE_PATH": str(output),
    }
    # Source only the function, not the operational health checks or a real network call.
    shell = f"set -u\nnow_iso='test'\n{match.group()}\nsend_telegram 'x' 'title' 'body'\n"
    proc = subprocess.run(["bash", "-c", shell], env=env, capture_output=True, text=True,
                          timeout=5, check=True)
    assert token not in proc.stdout + proc.stderr
    assert "synthetic_fake_token" not in proc.stdout + proc.stderr
    assert output.exists() == called
    if called:
        actual = json.loads(output.read_text())
        assert not actual["token_in_env"]
        assert all(token not in arg for arg in actual["argv"])
        assert actual["argv"][:2] == ["--config", "-"]
        assert actual["config"] == f'url = "https://api.telegram.org/bot{token}/sendMessage"\n'
        assert "chat_id=100" in actual["argv"]
        assert "message_thread_id=200" in actual["argv"]
        assert "parse_mode=HTML" in actual["argv"]
        assert "disable_web_page_preview=true" in actual["argv"]
        text_index = actual["argv"].index("--data-urlencode") + 1
        assert actual["argv"][text_index] == (
            "text=x <b>[monad-ops-watchdog]</b> title\n"
            "<i>external health check · test</i>\n\nbody"
        )
