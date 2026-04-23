"""Tests for ``TempCtrlClient`` driven against the dummy PicoManager.

The ``client`` fixture starts an in-process ``PicoManager`` with a
``DummyPicoPeltier`` backed by ``TempCtrlEmulator``. Tests inspect the
emulator's channel states (``lna``, ``load``) to confirm that commands
sent via :class:`TempCtrlClient` land with the right fields.
"""

import time

import pytest

from eigsep_observing import TempCtrlClient


SETTINGS = {
    "watchdog_timeout_ms": 25000,
    "LNA": {
        "enable": True,
        "target_C": 27.5,
        "hysteresis_C": 0.3,
        "clamp": 0.5,
    },
    "LOAD": {
        "enable": True,
        "target_C": 22.0,
        "hysteresis_C": 0.4,
        "clamp": 0.7,
    },
}


def _emulator(client):
    return client._manager.picos["tempctrl"]._emulator


def _wait_until(predicate, timeout=2.0, interval=0.02):
    """Poll ``predicate`` until it returns truthy or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_apply_settings_pushes_all_fields(client):
    """apply_settings walks watchdog → clamp → setpoint → enable, and
    every field ends up on the emulator. Commands are dispatched
    asynchronously through PicoManager's cmd_loop so poll for the
    final expected state."""
    tc = TempCtrlClient(client.transport, settings=SETTINGS)
    tc.apply_settings()
    em = _emulator(client)

    assert _wait_until(
        lambda: (
            em.watchdog_timeout_ms == 25000
            and em.lna.clamp == pytest.approx(0.5)
            and em.load.clamp == pytest.approx(0.7)
            and em.lna.T_target == pytest.approx(27.5)
            and em.load.T_target == pytest.approx(22.0)
            and em.lna.hysteresis == pytest.approx(0.3)
            and em.load.hysteresis == pytest.approx(0.4)
            and em.lna.enabled is True
            and em.load.enabled is True
        )
    ), (
        f"emulator state did not converge: watchdog={em.watchdog_timeout_ms}, "
        f"LNA(clamp={em.lna.clamp}, T_target={em.lna.T_target}, "
        f"hyst={em.lna.hysteresis}, enabled={em.lna.enabled}), "
        f"LOAD(clamp={em.load.clamp}, T_target={em.load.T_target}, "
        f"hyst={em.load.hysteresis}, enabled={em.load.enabled})"
    )


def test_apply_settings_empty_is_no_op(client):
    """Empty settings dict sends no commands — the pre-existing
    firmware state (defaults) is untouched."""
    em = _emulator(client)
    pre = (
        em.watchdog_timeout_ms,
        em.lna.T_target,
        em.load.T_target,
        em.lna.enabled,
        em.load.enabled,
    )
    tc = TempCtrlClient(client.transport, settings={})
    tc.apply_settings()
    # Nothing to wait for, but sleep a touch to make sure no command
    # snuck through.
    time.sleep(0.1)
    post = (
        em.watchdog_timeout_ms,
        em.lna.T_target,
        em.load.T_target,
        em.lna.enabled,
        em.load.enabled,
    )
    assert pre == post


def test_set_temperature_only_pushes_specified_channel(client):
    """Passing only T_LNA must leave the LOAD target untouched so a
    partial-apply caller doesn't accidentally rewrite the other side."""
    em = _emulator(client)
    load_before = em.load.T_target
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_temperature(T_LNA=31.0, LNA_hyst=0.25)
    assert _wait_until(
        lambda: (
            em.lna.T_target == pytest.approx(31.0)
            and em.lna.hysteresis == pytest.approx(0.25)
        )
    )
    assert em.load.T_target == pytest.approx(load_before)


def test_set_clamp_partial(client):
    em = _emulator(client)
    load_before = em.load.clamp
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_clamp(LNA=0.35)
    assert _wait_until(lambda: em.lna.clamp == pytest.approx(0.35))
    assert em.load.clamp == pytest.approx(load_before)


def test_set_enable_requires_at_least_one_channel(client):
    """With neither LNA nor LOAD specified, set_enable is a no-op —
    the firmware's default-True kwargs would otherwise silently arm
    both channels."""
    em = _emulator(client)
    pre = (em.lna.enabled, em.load.enabled)
    tc = TempCtrlClient(client.transport, settings=SETTINGS)
    tc.set_enable()
    time.sleep(0.1)
    assert (em.lna.enabled, em.load.enabled) == pre


def test_set_enable_uses_settings_for_unspecified_channel(client):
    """When only one channel is passed, the other falls back to the
    stored settings' ``enable`` — avoids silently arming LOAD when the
    caller only intended to touch LNA."""
    em = _emulator(client)
    settings = {
        "LNA": {"enable": False},
        "LOAD": {"enable": False},
    }
    tc = TempCtrlClient(client.transport, settings=settings)
    tc.set_enable(LNA=True)
    assert _wait_until(
        lambda: em.lna.enabled is True and em.load.enabled is False
    )


def test_set_watchdog_timeout(client):
    em = _emulator(client)
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_watchdog_timeout(12345)
    assert _wait_until(lambda: em.watchdog_timeout_ms == 12345)


def test_get_status_returns_snapshot_or_none(client):
    """get_status returns a dict with the full schema, or None before
    the pico has published. Fire a command first to ensure at least
    one publish has happened."""
    tc = TempCtrlClient(client.transport, settings=SETTINGS)
    tc.apply_settings()
    assert _wait_until(lambda: tc.get_status() is not None)
    status = tc.get_status()
    assert status["sensor_name"] == "tempctrl"
    assert "LNA_T_target" in status
    assert "LOAD_T_target" in status


def test_is_available_reflects_registration(client):
    tc = TempCtrlClient(client.transport)
    # Dummy manager registers ``tempctrl`` in ``DUMMY_PICO_CLASSES``.
    assert tc.is_available


@pytest.mark.parametrize(
    "bad_settings, needle",
    [
        ("not a dict", "must be a dict"),
        (["not", "a", "dict"], "must be a dict"),
        ({"LNA": "not a dict"}, "LNA"),
        ({"LNA": {"target_C": "twenty-five"}}, "target_C"),
        ({"LOAD": {"clamp": "oops"}}, "clamp"),
        ({"watchdog_timeout_ms": "forever"}, "watchdog_timeout_ms"),
        # YAML `enable: "false"` parses as the truthy string "False";
        # bool(...) would silently arm the channel. Reject it loudly.
        ({"LNA": {"enable": "false"}}, "enable"),
    ],
)
def test_coerce_settings_raises_on_bad_config(bad_settings, needle):
    """Bad yaml types surface as ``ValueError`` at construction time
    so :meth:`PandaClient.init_tempctrl` can disable the client loudly
    rather than the loop thread unwinding on the first apply."""
    with pytest.raises(ValueError, match=needle):
        TempCtrlClient._coerce_settings(bad_settings)


def test_coerce_settings_none_returns_empty():
    """Explicit ``None`` is the documented "no settings" sentinel."""
    assert TempCtrlClient._coerce_settings(None) == {}


def test_coerce_settings_normalizes_types():
    """Int literals in float fields are accepted and promoted to float;
    bool ``enable`` is preserved as-is."""
    out = TempCtrlClient._coerce_settings(
        {
            "watchdog_timeout_ms": 30000,
            "LNA": {
                "enable": True,
                "target_C": 25,  # int in a float field
                "clamp": 1,
            },
        }
    )
    assert out["watchdog_timeout_ms"] == 30000
    assert isinstance(out["LNA"]["target_C"], float)
    assert out["LNA"]["target_C"] == 25.0
    assert isinstance(out["LNA"]["clamp"], float)
    assert out["LNA"]["enable"] is True
