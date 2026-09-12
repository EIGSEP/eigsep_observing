"""Tests for ``TempCtrlClient`` driven against the dummy PicoManager.

The ``client`` fixture starts an in-process ``PicoManager`` with a
``DummyPicoTempCtrl`` backed by ``TempCtrlEmulator``. Tests inspect the
emulator's ``load`` channel state (the sole tempctrl channel — the
LNA/Peltier channel and its PI control were removed) to confirm that
commands sent via :class:`TempCtrlClient` land with the right fields.
"""

import time
from unittest.mock import patch

import pytest

from eigsep_observing import TempCtrlClient


SETTINGS = {
    "watchdog_timeout_ms": 25000,
    "LOAD": {
        "enable": True,
        "target_C": 22.0,
        "hysteresis_C": 0.4,
    },
}


def _emulator(client):
    return client._manager.picos["tempctrl"]._emulator


def _wait_until(predicate, timeout=5.0, interval=0.02):
    """Poll ``predicate`` until it returns truthy or ``timeout`` elapses.

    Default timeout is 5 s — the original 2 s was tight enough that under
    ``pytest -n auto`` on a busy CI runner, a test could intermittently
    miss the first tempctrl publish (200 ms emulator cadence x scheduler
    jitter). Healthy local runs complete in well under a second, so the
    bump only extends the worst case.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_apply_settings_pushes_all_fields(client):
    """apply_settings walks watchdog -> installed -> setpoint -> enable,
    and every field ends up on the emulator's LOAD channel."""
    tc = TempCtrlClient(client.transport, settings=SETTINGS)
    tc.apply_settings()
    em = _emulator(client)

    assert _wait_until(
        lambda: (
            em.watchdog_timeout_ms == 25000
            and em.load.T_target == pytest.approx(22.0)
            and em.load.hysteresis == pytest.approx(0.4)
            and em.load.enabled is True
        )
    ), (
        f"emulator state did not converge: watchdog={em.watchdog_timeout_ms}, "
        f"LOAD(T_target={em.load.T_target}, hyst={em.load.hysteresis}, "
        f"enabled={em.load.enabled})"
    )


def test_apply_settings_empty_is_no_op(client):
    """Empty settings dict sends no commands — the pre-existing
    firmware state (defaults) is untouched."""
    em = _emulator(client)
    pre = (em.watchdog_timeout_ms, em.load.T_target, em.load.enabled)
    tc = TempCtrlClient(client.transport, settings={})
    tc.apply_settings()
    # Nothing to wait for, but sleep a touch to make sure no command
    # snuck through.
    time.sleep(0.1)
    post = (em.watchdog_timeout_ms, em.load.T_target, em.load.enabled)
    assert pre == post


def test_set_temperature_only_pushes_when_t_load_given(client):
    """``set_temperature`` with ``T_LOAD=None`` must not send a command
    — a partial-application caller shouldn't accidentally rewrite the
    setpoint to firmware defaults."""
    tc = TempCtrlClient(client.transport, settings={})
    with patch.object(tc._proxy, "send_command") as send:
        tc.set_temperature()
    send.assert_not_called()


def test_set_temperature_pushes_to_emulator(client):
    em = _emulator(client)
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_temperature(T_LOAD=31.0, LOAD_hyst=0.25)
    assert _wait_until(
        lambda: (
            em.load.T_target == pytest.approx(31.0)
            and em.load.hysteresis == pytest.approx(0.25)
        )
    )


def test_set_enable_none_is_no_op(client):
    """``set_enable(LOAD=None)`` (the default) must not send a
    command — the firmware's own default-True kwarg would otherwise
    silently arm the channel."""
    tc = TempCtrlClient(client.transport, settings=SETTINGS)
    with patch.object(tc._proxy, "send_command") as send:
        tc.set_enable()
    send.assert_not_called()


def test_set_enable_pushes_to_emulator(client):
    em = _emulator(client)
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_enable(LOAD=True)
    assert _wait_until(lambda: em.load.enabled is True)


def test_set_watchdog_timeout(client):
    em = _emulator(client)
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_watchdog_timeout(12345)
    assert _wait_until(lambda: em.watchdog_timeout_ms == 12345)


def test_get_status_returns_snapshot_or_none(client):
    """get_status republishes the ``tempctrl_load`` stream under the
    flat ``LOAD_*`` shape callers (notably ``_tempctrl_health_check``)
    depend on. Returns None before the pico stream has published."""
    tc = TempCtrlClient(client.transport, settings=SETTINGS)
    tc.apply_settings()
    assert _wait_until(
        lambda: (s := tc.get_status()) is not None and "LOAD_T_target" in s
    )
    status = tc.get_status()
    assert "LOAD_T_target" in status
    assert "LOAD_status" in status
    assert "watchdog_timeout_ms" in status


def test_is_available_reflects_registration(client):
    tc = TempCtrlClient(client.transport)
    # Dummy manager registers ``tempctrl`` in ``DUMMY_PICO_CLASSES``.
    assert tc.is_available


@pytest.mark.parametrize(
    "bad_settings, needle",
    [
        ("not a dict", "must be a dict"),
        (["not", "a", "dict"], "must be a dict"),
        ({"LOAD": "not a dict"}, "LOAD"),
        ({"LOAD": {"target_C": "twenty-five"}}, "target_C"),
        ({"LOAD": {"hysteresis_C": "oops"}}, "hysteresis_C"),
        ({"watchdog_timeout_ms": "forever"}, "watchdog_timeout_ms"),
        # YAML `enable: "false"` parses as the truthy string "False";
        # bool(...) would silently arm the channel. Reject it loudly.
        ({"LOAD": {"enable": "false"}}, "enable"),
        # `installed: "false"` would silently keep the descoped channel
        # publishing its dead-divider error stream.
        ({"LOAD": {"installed": "false"}}, "installed"),
        # Config contradiction: an absent module cannot be armed.
        (
            {"LOAD": {"installed": False, "enable": True}},
            "cannot be armed",
        ),
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
    bool ``enable`` is preserved as-is. Settings not in the LOAD schema
    (e.g. a leftover ``clamp``/``Kp``/``Ki`` key from an old config) are
    silently ignored — ``_coerce_settings`` only reads the keys it
    knows about."""
    out = TempCtrlClient._coerce_settings(
        {
            "watchdog_timeout_ms": 30000,
            "LOAD": {
                "enable": True,
                "target_C": 25,  # int in a float field
                "hysteresis_C": 1,
                "clamp": 0.6,  # ignored: not a recognized LOAD field
                "Kp": 0.2,  # ignored: not a recognized LOAD field
            },
        }
    )
    assert out["watchdog_timeout_ms"] == 30000
    assert isinstance(out["LOAD"]["target_C"], float)
    assert out["LOAD"]["target_C"] == 25.0
    assert isinstance(out["LOAD"]["hysteresis_C"], float)
    assert out["LOAD"]["enable"] is True
    assert "clamp" not in out["LOAD"]
    assert "Kp" not in out["LOAD"]


def test_coerce_settings_accepts_installed_bool():
    """``installed`` rides the same strict-bool path as ``enable``;
    ``installed: false`` with ``enable: false`` is the valid descope
    shape."""
    out = TempCtrlClient._coerce_settings(
        {"LOAD": {"installed": False, "enable": False}},
    )
    assert out["LOAD"]["installed"] is False


def test_set_installed_pushes_to_emulator(client):
    """``set_installed`` flips the firmware-side installed flag; the
    fan-out then stops publishing the stream."""
    tc = TempCtrlClient(client.transport)
    em = _emulator(client)
    assert em.load.installed is True

    tc.set_installed(LOAD=False)

    assert _wait_until(lambda: em.load.installed is False)


def test_set_installed_no_kwargs_is_no_op(client):
    """``set_installed()`` with ``LOAD=None`` must not send a command
    — partial-application callers don't flip the untouched channel."""
    tc = TempCtrlClient(client.transport)
    with patch.object(tc._proxy, "send_command") as mock_send:
        tc.set_installed()
        mock_send.assert_not_called()


def test_apply_settings_order_watchdog_installed_temperature_enable():
    """``apply_settings`` pushes in the order watchdog -> installed ->
    temperature -> enable, mirroring ``PicoTempCtrl``'s reconnect
    replay order."""
    from eigsep_redis.testing import DummyTransport

    transport = DummyTransport()
    tc = TempCtrlClient(
        transport,
        settings={
            "watchdog_timeout_ms": 25000,
            "LOAD": {
                "installed": True,
                "enable": True,
                "target_C": 25.0,
                "hysteresis_C": 0.5,
            },
        },
    )
    sent = []
    with patch.object(
        tc._proxy,
        "send_command",
        side_effect=lambda cmd, **kw: sent.append(cmd),
    ):
        tc.apply_settings()
    assert sent == [
        "set_watchdog_timeout",
        "set_installed",
        "set_temperature",
        "set_enable",
    ]


def test_get_status_skips_uninstalled_channel_stream():
    """A channel whose settings say ``installed: false`` is never read:
    a leftover hash entry (lab bring-up, pre-descope deployment, reboot
    burst) must not resurrect stale data into the merged status or
    trigger the snapshot reader's staleness warning on every poll."""
    from eigsep_redis import MetadataWriter
    from eigsep_redis.testing import DummyTransport

    from eigsep_observing._test_fixtures import tempctrl_post_handler_reading

    transport = DummyTransport()
    writer = MetadataWriter(transport)
    writer.add("tempctrl_load", tempctrl_post_handler_reading("tempctrl_load"))

    tc = TempCtrlClient(
        transport,
        settings={"LOAD": {"installed": False, "enable": False}},
    )
    reads = []
    original_get = tc._reader.get

    def spying_get(key, *args, **kwargs):
        reads.append(key)
        return original_get(key, *args, **kwargs)

    with patch.object(tc._reader, "get", side_effect=spying_get):
        status = tc.get_status()

    assert "tempctrl_load" not in reads
    assert status is None
