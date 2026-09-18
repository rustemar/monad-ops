"""`monad-ops maintenance`: the window is a meta-table value the service reads."""

from __future__ import annotations

import argparse
import sqlite3
import time

import pytest

from monad_ops import cli
from monad_ops.storage import Storage


def _args(**kw) -> argparse.Namespace:
    base = {"config": None, "minutes": None, "off": False}
    base.update(kw)
    return argparse.Namespace(**base)


def _fake_config(path, enabled=True):
    class _Persistence:
        pass

    class _Config:
        persistence = _Persistence()

    _Config.persistence.path = path
    _Config.persistence.enabled = enabled
    return _Config()


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "state.db"
    Storage(path).close()  # the service's database exists
    monkeypatch.setattr(cli, "load_config", lambda _c: _fake_config(path))
    return path


@pytest.mark.asyncio
async def test_minutes_opens_a_window_and_records_when_it_opened(db, capsys) -> None:
    before = time.time()
    assert await cli._cmd_maintenance(_args(minutes=15)) == 0
    st = Storage(db)
    since, until = st.maintenance_window()
    st.close()
    assert since is not None and before - 1 <= since <= before + 60
    assert until is not None and 14 * 60 <= until - before <= 16 * 60
    assert "open until" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_extending_an_open_window_keeps_its_start(db) -> None:
    await cli._cmd_maintenance(_args(minutes=5))
    st = Storage(db)
    since1, _ = st.maintenance_window()
    st.close()
    await cli._cmd_maintenance(_args(minutes=30))
    st = Storage(db)
    since2, until2 = st.maintenance_window()
    st.close()
    assert since1 == since2 and until2 - time.time() > 25 * 60


@pytest.mark.asyncio
async def test_off_ends_the_window_now_but_leaves_the_summary_to_the_service(db, capsys) -> None:
    await cli._cmd_maintenance(_args(minutes=15))
    assert await cli._cmd_maintenance(_args(off=True)) == 0
    st = Storage(db)
    since, until = st.maintenance_window()
    st.close()
    assert since is not None  # cleared by the gate once it has summarised
    assert until is not None and until <= time.time()
    capsys.readouterr()
    assert await cli._cmd_maintenance(_args()) == 0
    assert "summary pending" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_off_without_a_window_writes_nothing(db, capsys) -> None:
    assert await cli._cmd_maintenance(_args(off=True)) == 0
    assert "no window open" in capsys.readouterr().err
    st = Storage(db)
    assert st.maintenance_window() == (None, None)
    st.close()


@pytest.mark.asyncio
async def test_non_positive_minutes_is_rejected(db) -> None:
    assert await cli._cmd_maintenance(_args(minutes=0)) == 2


@pytest.mark.asyncio
async def test_persistence_disabled_is_refused(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli, "load_config", lambda _c: _fake_config(tmp_path / "state.db", enabled=False)
    )
    assert await cli._cmd_maintenance(_args(minutes=15)) == 2
    assert "persistence" in capsys.readouterr().err
    assert not (tmp_path / "state.db").exists()


@pytest.mark.asyncio
async def test_missing_database_is_refused_and_not_created(tmp_path, monkeypatch, capsys) -> None:
    """Run from the wrong directory the CLI must not conjure an empty state.db
    the service will never read."""
    path = tmp_path / "elsewhere" / "state.db"
    monkeypatch.setattr(cli, "load_config", lambda _c: _fake_config(path))
    assert await cli._cmd_maintenance(_args(minutes=15)) == 2
    assert "no database" in capsys.readouterr().err
    assert not path.exists()


@pytest.mark.asyncio
async def test_locked_database_gives_a_retry_hint_not_a_traceback(db, monkeypatch, capsys) -> None:
    def _locked(self, *a, **kw):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(Storage, "open_maintenance", _locked)
    assert await cli._cmd_maintenance(_args(minutes=15)) == 1
    assert "busy" in capsys.readouterr().err
