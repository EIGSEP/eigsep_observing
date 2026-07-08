import subprocess

import pytest

from eigsep_observing import vna_service


def test_start_runs_systemctl_start(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    vna_service.start()
    assert calls == [
        ["systemctl", "start", "--no-ask-password", "cmtvna.service"]
    ]


def test_start_falls_back_to_sudo(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        rc = 0 if cmd[0] == "sudo" else 1
        return subprocess.CompletedProcess(cmd, rc, "", "boom")

    monkeypatch.setattr(subprocess, "run", fake_run)
    vna_service.start()
    assert calls[0][0] == "systemctl"
    assert calls[1][:2] == ["sudo", "-n"]


def test_start_raises_when_both_fail(monkeypatch):
    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 1, "", "nope")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="start cmtvna.service failed"):
        vna_service.start()


def test_wait_ready_returns_idn_after_retries(monkeypatch):
    attempts = {"n": 0}

    class FakeResource:
        read_termination = None
        timeout = None

        def query(self, _msg):
            return "CMT,R60,123,1.7.1\n"

        def close(self):
            pass

    class FakeRM:
        def open_resource(self, _addr):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise OSError("connection refused")
            return FakeResource()

    monkeypatch.setattr(
        vna_service.pyvisa, "ResourceManager", lambda _b: FakeRM()
    )
    monkeypatch.setattr(vna_service.time, "sleep", lambda _s: None)

    idn = vna_service.wait_ready("127.0.0.1", 5025, timeout=30.0)
    assert idn == "CMT,R60,123,1.7.1"
    assert attempts["n"] == 3


def test_wait_ready_times_out(monkeypatch):
    clock = {"t": 0.0}

    class FakeRM:
        def open_resource(self, _addr):
            raise OSError("refused")

    monkeypatch.setattr(
        vna_service.pyvisa, "ResourceManager", lambda _b: FakeRM()
    )
    monkeypatch.setattr(vna_service.time, "sleep", lambda _s: None)

    def fake_monotonic():
        clock["t"] += 1.0
        return clock["t"]

    monkeypatch.setattr(vna_service.time, "monotonic", fake_monotonic)
    with pytest.raises(TimeoutError, match="cmtvna not ready"):
        vna_service.wait_ready("127.0.0.1", 5025, timeout=2.0)


def test_wait_ready_closes_resource_on_query_failure(monkeypatch):
    closed = {"n": 0}
    attempts = {"n": 0}

    class FakeResource:
        read_termination = None
        timeout = None

        def query(self, _msg):
            attempts["n"] += 1
            if attempts["n"] < 2:
                raise OSError("instrument not ready")
            return "CMT,R60,1,1.7.1\n"

        def close(self):
            closed["n"] += 1

    class FakeRM:
        def open_resource(self, _addr):
            return FakeResource()

    monkeypatch.setattr(
        vna_service.pyvisa, "ResourceManager", lambda _b: FakeRM()
    )
    monkeypatch.setattr(vna_service.time, "sleep", lambda _s: None)

    idn = vna_service.wait_ready("127.0.0.1", 5025, timeout=30.0)
    assert idn == "CMT,R60,1,1.7.1"
    # closed on the failed attempt AND the successful one
    assert closed["n"] == 2
