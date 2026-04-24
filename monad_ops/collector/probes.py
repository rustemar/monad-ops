"""Periodic local probes — ops-level health of the node host.

Each probe returns a ``ProbeResult`` that the dashboard and rules engine
can consume. Probes are cheap (well under a second each) and run in a
single background task on a fixed interval.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


_TRIEDB_DEVICE = Path("/dev/triedb")
_MONAD_CONFIG = Path("/home/monad/monad-bft/config/node.toml")
_MONAD_SECP_BACKUP = Path("/opt/monad/backup/secp-backup")
_MONAD_BLS_BACKUP = Path("/opt/monad/backup/bls-backup")

# Paths to watch for disk_usage. /home/monad holds the ledger (block_db);
# /opt/monad holds key backups; / is the OS root. TrieDB itself lives on
# a raw block device (see probe_triedb_device) so filesystem-level
# accounting doesn't apply there — triedb chunk exhaustion is a separate
# signal (emitted by monad-execution, e.g. "Disk usage: 0.9999" in the
# 0xAN Nodes.Guru halt on 2026-04-14).
_DISK_WATCH = [
    Path("/"),
    Path("/home/monad"),
    Path("/opt/monad"),
]
_DISK_WARN_PCT = 0.85
_DISK_CRITICAL_PCT = 0.95

# File-descriptor limit to require on monad-execution. A TN1 operator
# hit "Too many open files" on 2025-11-25 because nofile was 1024;
# upstream guidance points at 1048576 and the official Docker solonet
# compose uses 16384. We warn below 16k, critical below 4k.
_FD_LIMIT_WARN = 16_384
_FD_LIMIT_CRITICAL = 4_096


@dataclass(frozen=True, slots=True)
class ProbeResult:
    name: str
    status: str          # "ok" | "warn" | "critical" | "unknown"
    summary: str         # short human-readable line
    details: dict        # structured extras


async def _run(cmd: list[str], timeout: float = 5.0) -> tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")
    except (FileNotFoundError, asyncio.TimeoutError) as e:
        return 127, "", str(e)


# ─── services ─────────────────────────────────────────────────────────

async def probe_services(services: list[str]) -> ProbeResult:
    states: dict[str, str] = {}
    probe_errors: list[str] = []
    for svc in services:
        rc, out, _ = await _run(["systemctl", "is-active", svc])
        state = out.strip()
        # rc=127 from _run means TimeoutError or FileNotFoundError — we
        # did not actually observe the service state. Distinguish this
        # from the real "inactive" rc=3 (systemctl returns non-zero for
        # inactive/failed/etc., but stdout still contains the true state).
        # The 2026-04-20 stress test exposed this: when the event loop
        # was frozen under sqlite load, subprocess_exec calls timed out
        # → probe went critical "not active" even though services had
        # months of uptime. Record the error separately; fail quiet.
        if rc == 127 and not state:
            probe_errors.append(svc)
            states[svc] = "probe_error"
        else:
            states[svc] = state or f"rc={rc}"

    bad = [s for s, state in states.items()
           if state not in ("active", "probe_error")]

    if probe_errors:
        return ProbeResult(
            name="services",
            status="unknown",
            summary=(
                f"probe timed out for {len(probe_errors)}/{len(services)} "
                f"service(s) — cannot evaluate"
            ),
            details=states,
        )
    if not bad:
        return ProbeResult(
            name="services",
            status="ok",
            summary=f"{len(services)}/{len(services)} active",
            details=states,
        )
    return ProbeResult(
        name="services",
        status="critical",
        summary=f"{len(bad)} service(s) not active: {', '.join(bad)}",
        details=states,
    )


# ─── key backups ──────────────────────────────────────────────────────

async def probe_key_backups(
    paths: list[Path] | None = None,
    warn_after_days: int = 30,
) -> ProbeResult:
    paths = paths or [_MONAD_SECP_BACKUP, _MONAD_BLS_BACKUP]
    details: dict = {}
    missing: list[str] = []
    stale: list[str] = []
    now = time.time()

    for p in paths:
        try:
            st = p.stat()
            age_days = (now - st.st_mtime) / 86400.0
            details[str(p)] = {
                "exists": True,
                "size": st.st_size,
                "mtime": int(st.st_mtime),
                "age_days": round(age_days, 1),
            }
            if age_days > warn_after_days:
                stale.append(f"{p.name} ({age_days:.0f}d)")
        except FileNotFoundError:
            details[str(p)] = {"exists": False}
            missing.append(p.name)
        except PermissionError as e:
            details[str(p)] = {"exists": "unknown", "error": str(e)}

    if missing:
        return ProbeResult(
            name="key_backups",
            status="critical",
            summary=f"missing: {', '.join(missing)}",
            details=details,
        )
    if stale:
        return ProbeResult(
            name="key_backups",
            status="warn",
            summary=f"stale (> {warn_after_days}d): {', '.join(stale)}",
            details=details,
        )
    return ProbeResult(
        name="key_backups",
        status="ok",
        summary=f"{len(paths)} backup(s) present",
        details=details,
    )


# ─── NVMe DISCARD / SMART ─────────────────────────────────────────────

async def probe_triedb_device() -> ProbeResult:
    """Check the device backing TrieDB supports DISCARD and is healthy."""
    if not _TRIEDB_DEVICE.exists():
        return ProbeResult(
            name="triedb_device",
            status="unknown",
            summary=f"{_TRIEDB_DEVICE} not found",
            details={},
        )

    # Resolve /dev/triedb → underlying block device (symlink on most setups).
    try:
        real = os.path.realpath(_TRIEDB_DEVICE)
    except OSError:
        real = str(_TRIEDB_DEVICE)

    details: dict = {"triedb_link": str(_TRIEDB_DEVICE), "underlying": real}

    # DISCARD support via lsblk.
    rc, out, _ = await _run(["lsblk", "--discard", "--json", real])
    disc_ok = False
    # Distinguish "lsblk timed out / not installed" from "lsblk said disc=0".
    # Same reasoning as probe_services: during event-loop freeze the
    # subprocess call times out (rc=127 via _run) and we must NOT raise
    # a critical — our tooling not the node is the problem. The running
    # node is definitive proof DISCARD worked at init, so timeout here
    # should be silent (unknown).
    probe_timeout = (rc == 127 and not out)
    if rc == 0 and out:
        try:
            data = json.loads(out)
            for bd in data.get("blockdevices", []):
                disc_gran = bd.get("disc-gran") or "0"
                disc_max = bd.get("disc-max") or "0"
                details["disc_gran"] = disc_gran
                details["disc_max"] = disc_max
                if disc_gran not in ("0", "0B", None) and disc_max not in ("0", "0B", None):
                    disc_ok = True
        except Exception as e:  # noqa: BLE001
            details["lsblk_parse_error"] = str(e)

    # SMART health — best-effort; /usr/sbin/nvme may not be installed.
    rc, out, _ = await _run(["nvme", "smart-log", real, "-o", "json"])
    smart_ok: bool | None = None
    if rc == 0 and out:
        try:
            smart = json.loads(out)
            # Common fields: critical_warning, temperature, percentage_used,
            # available_spare.
            crit = smart.get("critical_warning", 0)
            used_pct = smart.get("percentage_used")
            spare = smart.get("available_spare")
            temp_k = smart.get("temperature")
            details["smart_critical_warning"] = crit
            details["smart_percentage_used"] = used_pct
            details["smart_available_spare"] = spare
            if temp_k is not None:
                details["smart_temperature_c"] = round(temp_k - 273.15, 1)
            smart_ok = crit == 0 and (used_pct is None or used_pct < 80) \
                and (spare is None or spare > 20)
        except Exception as e:  # noqa: BLE001
            details["nvme_parse_error"] = str(e)

    if probe_timeout:
        return ProbeResult(
            name="triedb_device",
            status="unknown",
            summary="lsblk probe timed out — cannot evaluate (node may still be healthy)",
            details=details,
        )
    if not disc_ok:
        return ProbeResult(
            name="triedb_device",
            status="critical",
            summary="BLKDISCARD not supported on TrieDB device — TrieDB init would fail",
            details=details,
        )
    if smart_ok is False:
        return ProbeResult(
            name="triedb_device",
            status="warn",
            summary="DISCARD ok; NVMe SMART shows wear or warning",
            details=details,
        )
    return ProbeResult(
        name="triedb_device",
        status="ok",
        summary="DISCARD supported" + (" + SMART clean" if smart_ok else ""),
        details=details,
    )


# ─── UDP authenticated config ─────────────────────────────────────────

async def probe_udp_config() -> ProbeResult:
    """Authenticated UDP enforcement is rolling out (2026-03-25 announcement).

    Preferred path: parse node.toml for ``authenticated_bind_address_port``.
    Fallback path (when node.toml is mode-locked to the monad user):
    check if *any* UDP socket is bound on the standard auth port (8001).
    """
    # Try to read the config first — most reliable.
    try:
        if _MONAD_CONFIG.exists():
            content = _MONAD_CONFIG.read_text()
            auth_match = re.search(r"authenticated_bind_address_port\s*=\s*(\d+)", content)
            clear_match = re.search(r"bind_address_port\s*=\s*(\d+)", content)
            if auth_match:
                return ProbeResult(
                    name="udp_config",
                    status="ok",
                    summary=f"authenticated UDP configured on :{auth_match.group(1)}",
                    details={
                        "auth_port": int(auth_match.group(1)),
                        "clear_port": int(clear_match.group(1)) if clear_match else None,
                        "source": "node.toml",
                    },
                )
            return ProbeResult(
                name="udp_config",
                status="warn",
                summary="node.toml missing authenticated_bind_address_port (2026-03-25 deprecation)",
                details={"source": "node.toml"},
            )
    except PermissionError:
        # Fall through to socket-based fallback.
        pass

    # Socket fallback: 8001 listening UDP suggests authenticated bind is up.
    rc, out, _ = await _run(["ss", "-ulnp"])
    has_8001 = ":8001" in out
    has_8000 = ":8000" in out
    if has_8001:
        return ProbeResult(
            name="udp_config",
            status="ok",
            summary="authenticated UDP port :8001 is listening (config not readable)",
            details={"auth_port": 8001, "clear_port": 8000 if has_8000 else None, "source": "socket-fallback"},
        )
    return ProbeResult(
        name="udp_config",
        status="unknown",
        summary="node.toml not readable and :8001 not detected",
        details={"hint": "grant group read to node.toml or align the service user with the file's owner/group"},
    )


# ─── disk usage (OS filesystem) ───────────────────────────────────────

async def probe_disk_usage(paths: list[Path] | None = None) -> ProbeResult:
    """Watch filesystem usage on paths that matter for node liveness.

    Caught: ledger/block_db growth, OS fill from journald runaway, key
    backup partition fill. NOT caught: TrieDB raw-device chunk
    exhaustion — that's a separate failure mode (monad-execution emits
    its own "Disk usage:" line in the journal).
    """
    paths = paths or _DISK_WATCH
    details: dict = {}
    worst_status = "ok"
    worst_msg = ""

    for p in paths:
        if not p.exists():
            details[str(p)] = {"exists": False}
            continue
        try:
            usage = shutil.disk_usage(p)
        except OSError as e:
            details[str(p)] = {"exists": True, "error": str(e)}
            continue
        used_ratio = usage.used / usage.total if usage.total else 0.0
        entry = {
            "exists": True,
            "total_gb": round(usage.total / 1e9, 1),
            "used_gb": round(usage.used / 1e9, 1),
            "free_gb": round(usage.free / 1e9, 1),
            "used_ratio": round(used_ratio, 4),
        }
        details[str(p)] = entry

        if used_ratio >= _DISK_CRITICAL_PCT:
            worst_status = "critical"
            worst_msg = f"{p} {used_ratio:.1%} used ({entry['free_gb']} GB free)"
        elif used_ratio >= _DISK_WARN_PCT and worst_status != "critical":
            worst_status = "warn"
            worst_msg = f"{p} {used_ratio:.1%} used"

    if worst_status == "ok":
        highest = max(
            (d.get("used_ratio", 0.0) for d in details.values() if d.get("exists")),
            default=0.0,
        )
        return ProbeResult(
            name="disk_usage",
            status="ok",
            summary=f"all mounts < {int(_DISK_WARN_PCT * 100)}% (peak {highest:.0%})",
            details=details,
        )
    return ProbeResult(
        name="disk_usage",
        status=worst_status,
        summary=worst_msg,
        details=details,
    )


# ─── file descriptor limits on monad-execution ────────────────────────

async def probe_fd_limits(service: str = "monad-execution") -> ProbeResult:
    """Check nofile limit for the monad-execution process.

    A 1024 default breaks the node under load (observed on TN1
    2025-11-25 as "Too many open files"); official solonet compose
    mandates 16384 and upstream guidance points at 1048576.
    """
    rc, out, _ = await _run(
        ["systemctl", "show", service, "--property=MainPID", "--value"]
    )
    if rc != 0 or not out.strip() or out.strip() == "0":
        return ProbeResult(
            name="fd_limits",
            status="unknown",
            summary=f"{service} MainPID not resolvable (service down?)",
            details={"rc": rc, "raw": out.strip()},
        )

    pid = out.strip()
    limits_path = Path(f"/proc/{pid}/limits")
    if not limits_path.exists():
        return ProbeResult(
            name="fd_limits",
            status="unknown",
            summary=f"/proc/{pid}/limits not readable",
            details={"pid": pid},
        )

    try:
        content = limits_path.read_text()
    except (PermissionError, OSError) as e:
        return ProbeResult(
            name="fd_limits",
            status="unknown",
            summary=f"cannot read /proc/{pid}/limits: {e}",
            details={"pid": pid},
        )

    # "Max open files            1024                 4096                 files"
    match = re.search(r"Max open files\s+(\d+)\s+(\d+)", content)
    if not match:
        return ProbeResult(
            name="fd_limits",
            status="unknown",
            summary="Max open files row not found in /proc/<pid>/limits",
            details={"pid": pid},
        )

    soft = int(match.group(1))
    hard = int(match.group(2))
    details = {"pid": pid, "soft": soft, "hard": hard}

    if soft < _FD_LIMIT_CRITICAL:
        return ProbeResult(
            name="fd_limits",
            status="critical",
            summary=f"{service} nofile soft={soft} < {_FD_LIMIT_CRITICAL} — EMFILE imminent under load",
            details=details,
        )
    if soft < _FD_LIMIT_WARN:
        return ProbeResult(
            name="fd_limits",
            status="warn",
            summary=f"{service} nofile soft={soft} < {_FD_LIMIT_WARN} (recommended)",
            details=details,
        )
    return ProbeResult(
        name="fd_limits",
        status="ok",
        summary=f"{service} nofile soft={soft} hard={hard}",
        details=details,
    )


# ─── run all probes ───────────────────────────────────────────────────

async def run_all_probes(services: list[str]) -> list[ProbeResult]:
    return list(await asyncio.gather(
        probe_services(services),
        probe_key_backups(),
        probe_triedb_device(),
        probe_udp_config(),
        probe_disk_usage(),
        probe_fd_limits(),
    ))
