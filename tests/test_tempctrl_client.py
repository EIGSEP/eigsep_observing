"""Tests for ``TempCtrlClient`` driven against the dummy PicoManager.

The ``client`` fixture starts an in-process ``PicoManager`` with a
``DummyPicoPeltier`` backed by ``TempCtrlEmulator``. Tests inspect the
emulator's channel states (``lna``, ``load``) to confirm that commands
sent via :class:`TempCtrlClient` land with the right fields.
"""

import time
from unittest.mock import patch

import pytest

from eigsep_observing import TempCtrlClient


SETTINGS = {
    "watchdog_timeout_ms": 25000,
    "LNA": {
        "enable": True,
        "target_C": 27.5,
        "hysteresis_C": 0.3,
        "clamp": 0.5,
        "Kp": 0.25,
        "Ki": 0.01,
    },
    "LOAD": {
        "enable": True,
        "target_C": 22.0,
        "hysteresis_C": 0.4,
        "clamp": 0.7,
        "Kp": 0.18,
        "Ki": 0.02,
    },
}


def _emulator(client):
    return client._manager.picos["tempctrl"]._emulator


def _wait_until(predicate, timeout=5.0, interval=0.02):
    """Poll ``predicate`` until it returns truthy or ``timeout`` elapses.

    Default timeout is 5 s — the original 2 s was tight enough that under
    ``pytest -n auto`` on a busy CI runner, ``test_get_status_returns_
    snapshot_or_none`` would intermittently miss the first tempctrl
    publish (200 ms emulator cadence × per-channel × scheduler jitter).
    Healthy local runs complete in well under a second, so the bump only
    extends the worst case.
    """
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
            and em.lna.Kp == pytest.approx(0.25)
            and em.lna.Ki == pytest.approx(0.01)
            and em.load.Kp == pytest.approx(0.18)
            and em.load.Ki == pytest.approx(0.02)
            and em.lna.enabled is True
            and em.load.enabled is True
        )
    ), (
        f"emulator state did not converge: watchdog={em.watchdog_timeout_ms}, "
        f"LNA(clamp={em.lna.clamp}, T_target={em.lna.T_target}, "
        f"hyst={em.lna.hysteresis}, Kp={em.lna.Kp}, Ki={em.lna.Ki}, "
        f"enabled={em.lna.enabled}), "
        f"LOAD(clamp={em.load.clamp}, T_target={em.load.T_target}, "
        f"hyst={em.load.hysteresis}, Kp={em.load.Kp}, Ki={em.load.Ki}, "
        f"enabled={em.load.enabled})"
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


def test_set_gains_pushes_to_emulator(client):
    """Full-kwargs set_gains lands on both channels."""
    em = _emulator(client)
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_gains(LNA_Kp=0.3, LNA_Ki=0.02, LOAD_Kp=0.15, LOAD_Ki=0.005)
    assert _wait_until(
        lambda: (
            em.lna.Kp == pytest.approx(0.3)
            and em.lna.Ki == pytest.approx(0.02)
            and em.load.Kp == pytest.approx(0.15)
            and em.load.Ki == pytest.approx(0.005)
        )
    ), (
        f"gains did not land: LNA(Kp={em.lna.Kp}, Ki={em.lna.Ki}), "
        f"LOAD(Kp={em.load.Kp}, Ki={em.load.Ki})"
    )


def test_set_gains_partial_leaves_other_channel(client):
    """Partial-kwargs set_gains touches only the named channel."""
    em = _emulator(client)
    load_Kp_before = em.load.Kp
    load_Ki_before = em.load.Ki
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_gains(LNA_Kp=0.4, LNA_Ki=0.03)
    assert _wait_until(
        lambda: (
            em.lna.Kp == pytest.approx(0.4)
            and em.lna.Ki == pytest.approx(0.03)
        )
    )
    assert em.load.Kp == pytest.approx(load_Kp_before)
    assert em.load.Ki == pytest.approx(load_Ki_before)


def test_set_gains_no_kwargs_is_no_op(client):
    """``set_gains()`` with everything None must not send a command —
    otherwise the proxy round-trip would fire for nothing."""
    tc = TempCtrlClient(client.transport, settings={})
    with patch.object(tc._proxy, "send_command") as send:
        tc.set_gains()
    send.assert_not_called()


def test_reset_integral_clears_channel(client):
    """``reset_integral(LNA=True)`` zeroes the LNA accumulator via the
    emulator's reset hook. (LOAD-channel isolation is verified at the
    proxy boundary in ``test_reset_integral_sends_per_channel_kwargs``;
    the disabled-channel reset that the emulator runs every tick would
    mask any direct LOAD-state assertion here.)"""
    em = _emulator(client)
    em.lna.integral = 1.234
    tc = TempCtrlClient(client.transport, settings={})
    tc.reset_integral(LNA=True)
    assert _wait_until(lambda: em.lna.integral == pytest.approx(0.0))


def test_reset_integral_sends_per_channel_kwargs(client):
    """Spy on the proxy: ``reset_integral`` must forward exactly the
    channel kwargs it was called with (so LOAD stays untouched on an
    LNA-only reset)."""
    tc = TempCtrlClient(client.transport, settings={})
    with patch.object(tc._proxy, "send_command") as send:
        tc.reset_integral(LNA=True)
    send.assert_called_once_with("reset_integral", LNA=True, LOAD=False)


def test_reset_integral_no_channels_is_no_op(client):
    """Both channels False must skip the command — otherwise an
    operator press with no channel selected would silently bounce
    nothing across the proxy."""
    tc = TempCtrlClient(client.transport, settings={})
    with patch.object(tc._proxy, "send_command") as send:
        tc.reset_integral()
    send.assert_not_called()


def test_set_watchdog_timeout(client):
    em = _emulator(client)
    tc = TempCtrlClient(client.transport, settings={})
    tc.set_watchdog_timeout(12345)
    assert _wait_until(lambda: em.watchdog_timeout_ms == 12345)


def test_get_status_returns_snapshot_or_none(client):
    """get_status merges the two split Redis streams back into the flat
    LNA_*/LOAD_* shape callers (notably _tempctrl_health_check) depend
    on. Returns None before either pico stream has published."""
    tc = TempCtrlClient(client.transport, settings=SETTINGS)
    tc.apply_settings()
    # get_status() goes non-None as soon as EITHER pico stream
    # (tempctrl_lna OR tempctrl_load) has published, but the asserts
    # below need both. Under pytest -n auto the two streams can land
    # in separate ticks, so wait until the merged dict carries both
    # prefixes before snapshotting.
    assert _wait_until(
        lambda: (
            (s := tc.get_status()) is not None
            and "LNA_T_target" in s
            and "LOAD_T_target" in s
        )
    )
    status = tc.get_status()
    # The merge preserves the legacy flat shape: device-wide watchdog
    # fields at the top, per-channel fields under LNA_*/LOAD_* prefix.
    assert "LNA_T_target" in status
    assert "LOAD_T_target" in status
    assert "LNA_status" in status
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
        ({"LNA": "not a dict"}, "LNA"),
        ({"LNA": {"target_C": "twenty-five"}}, "target_C"),
        ({"LOAD": {"clamp": "oops"}}, "clamp"),
        ({"watchdog_timeout_ms": "forever"}, "watchdog_timeout_ms"),
        # YAML `enable: "false"` parses as the truthy string "False";
        # bool(...) would silently arm the channel. Reject it loudly.
        ({"LNA": {"enable": "false"}}, "enable"),
        ({"LNA": {"Kp": "tiny"}}, "Kp"),
        ({"LOAD": {"Ki": [0.0, 0.1]}}, "Ki"),
        # `cooling_enabled: "false"` has the same trap as `enable`: a
        # non-bool truthy value would silently leave the cooling guard
        # *armed* (i.e. the safety setting wouldn't apply).
        ({"LNA": {"cooling_enabled": "false"}}, "cooling_enabled"),
        # `installed: "false"` would silently keep the descoped channel
        # publishing its dead-divider error stream.
        ({"LNA": {"installed": "false"}}, "installed"),
        # Config contradiction: an absent module cannot be armed.
        (
            {"LNA": {"installed": False, "enable": True}},
            "cannot be armed",
        ),
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
    bool ``enable`` is preserved as-is. Kp/Ki ride the same float-coerce
    path as the other numeric fields. ``cooling_enabled`` rides the same
    strict-bool path as ``enable``."""
    out = TempCtrlClient._coerce_settings(
        {
            "watchdog_timeout_ms": 30000,
            "LNA": {
                "enable": True,
                "cooling_enabled": False,
                "target_C": 25,  # int in a float field
                "clamp": 1,
                "Kp": 0,  # int in a float field
                "Ki": 0,
            },
        }
    )
    assert out["watchdog_timeout_ms"] == 30000
    assert isinstance(out["LNA"]["target_C"], float)
    assert out["LNA"]["target_C"] == 25.0
    assert isinstance(out["LNA"]["clamp"], float)
    assert out["LNA"]["enable"] is True
    assert out["LNA"]["cooling_enabled"] is False
    assert isinstance(out["LNA"]["Kp"], float)
    assert out["LNA"]["Kp"] == 0.0
    assert isinstance(out["LNA"]["Ki"], float)
    assert out["LNA"]["Ki"] == 0.0


def test_coerce_settings_accepts_installed_bool():
    """``installed`` rides the same strict-bool path as ``enable``;
    ``installed: false`` with ``enable: false`` is the valid descope
    shape."""
    out = TempCtrlClient._coerce_settings(
        {
            "LNA": {"installed": False, "enable": False},
            "LOAD": {"installed": True, "enable": True},
        }
    )
    assert out["LNA"]["installed"] is False
    assert out["LOAD"]["installed"] is True


def test_set_installed_pushes_to_emulator(client):
    """``set_installed`` flips the firmware-side installed flag on the
    addressed channel(s); the fan-out then stops publishing that
    channel's stream."""
    tc = TempCtrlClient(client.transport)
    em = _emulator(client)
    assert em.lna.installed is True
    assert em.load.installed is True

    tc.set_installed(LNA=False)

    assert _wait_until(lambda: em.lna.installed is False)
    # LOAD was never sent; firmware default (True) untouched.
    assert em.load.installed is True


def test_set_installed_no_kwargs_is_no_op(client):
    """``set_installed()`` with both args None must not send a command
    — partial-application callers don't flip the untouched channel."""
    tc = TempCtrlClient(client.transport)
    with patch.object(tc._proxy, "send_command") as mock_send:
        tc.set_installed()
        mock_send.assert_not_called()


def test_apply_settings_order_installed_after_watchdog():
    """``apply_settings`` pushes installed right after the watchdog —
    before anything drive-producing — mirroring PicoPeltier's
    reconnect replay order."""
    from eigsep_redis.testing import DummyTransport

    transport = DummyTransport()
    tc = TempCtrlClient(
        transport,
        settings={
            "watchdog_timeout_ms": 25000,
            "LNA": {
                "installed": False,
                "enable": False,
                "target_C": 25.0,
                "clamp": 0.5,
            },
            "LOAD": {
                "installed": True,
                "enable": True,
                "cooling_enabled": False,
                "target_C": 25.0,
                "clamp": 0.5,
                "Kp": 0.3,
                "Ki": 0.01,
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
        "set_clamp",
        "set_cooling_enabled",
        "set_gains",
        "set_temperature",
        "set_enable",
    ]


@pytest.mark.parametrize(
    "off_ch, on_ch",
    [("LNA", "LOAD"), ("LOAD", "LNA")],
)
def test_get_status_skips_uninstalled_channel_stream(off_ch, on_ch):
    """A channel whose settings say ``installed: false`` is never read:
    a leftover hash entry (lab bring-up, pre-descope deployment, reboot
    burst) must not resurrect stale data into the merged status or
    trigger the snapshot reader's staleness warning on every poll."""
    from eigsep_redis import MetadataWriter
    from eigsep_redis.testing import DummyTransport

    from eigsep_observing._test_fixtures import tempctrl_post_handler_reading

    transport = DummyTransport()
    writer = MetadataWriter(transport)
    for stream in ("tempctrl_lna", "tempctrl_load"):
        writer.add(stream, tempctrl_post_handler_reading(stream))

    tc = TempCtrlClient(
        transport,
        settings={off_ch: {"installed": False, "enable": False}},
    )
    reads = []
    original_get = tc._reader.get

    def spying_get(key, *args, **kwargs):
        reads.append(key)
        return original_get(key, *args, **kwargs)

    with patch.object(tc._reader, "get", side_effect=spying_get):
        status = tc.get_status()

    assert f"tempctrl_{off_ch.lower()}" not in reads
    assert status is not None
    assert not any(k.startswith(f"{off_ch}_") for k in status)
    assert any(k.startswith(f"{on_ch}_") for k in status)
    # Device-wide watchdog fields still ride the installed channel.
    assert "watchdog_timeout_ms" in status


def test_set_cooling_enabled_pushes_to_emulator(client):
    """``set_cooling_enabled`` flips the firmware-side cooling_enabled
    flag on the addressed channel(s)."""
    tc = TempCtrlClient(client.transport)
    em = _emulator(client)
    assert em.lna.cooling_enabled is True
    assert em.load.cooling_enabled is True

    tc.set_cooling_enabled(LNA=False, LOAD=True)

    assert _wait_until(
        lambda: (
            em.lna.cooling_enabled is False and em.load.cooling_enabled is True
        )
    ), (
        f"cooling_enabled did not propagate: "
        f"LNA={em.lna.cooling_enabled}, LOAD={em.load.cooling_enabled}"
    )


def test_set_cooling_enabled_partial_leaves_other_channel(client):
    """Partial-kwargs ``set_cooling_enabled`` touches only the named
    channel (matches ``set_clamp`` / ``set_gains`` shape)."""
    tc = TempCtrlClient(client.transport)
    em = _emulator(client)

    tc.set_cooling_enabled(LNA=False)

    assert _wait_until(lambda: em.lna.cooling_enabled is False)
    # LOAD was never sent; firmware default (True) untouched.
    assert em.load.cooling_enabled is True


def test_set_cooling_enabled_no_kwargs_is_no_op(client):
    """``set_cooling_enabled()`` with both args None must not send a
    command — partial-application callers don't flip the untouched
    channel."""
    tc = TempCtrlClient(client.transport)
    with patch.object(tc._proxy, "send_command") as mock_send:
        tc.set_cooling_enabled()
        mock_send.assert_not_called()


def test_apply_settings_applies_cooling_enabled(client):
    """``cooling_enabled`` field in the yaml settings reaches the
    emulator via ``apply_settings``."""
    settings = {
        "LNA": {"enable": True, "cooling_enabled": False, "target_C": 25.0},
        "LOAD": {"enable": True, "cooling_enabled": True, "target_C": 25.0},
    }
    tc = TempCtrlClient(client.transport, settings=settings)
    tc.apply_settings()
    em = _emulator(client)
    assert _wait_until(
        lambda: (
            em.lna.cooling_enabled is False and em.load.cooling_enabled is True
        )
    ), (
        f"apply_settings did not propagate cooling_enabled: "
        f"LNA={em.lna.cooling_enabled}, LOAD={em.load.cooling_enabled}"
    )


def test_apply_settings_order_cooling_before_gains():
    """``apply_settings`` pushes cooling_enabled between clamp and
    gains so the asymmetric-clamp safety setting is in place before
    the PI controller can produce drive on the new config."""
    from eigsep_redis.testing import DummyTransport

    transport = DummyTransport()
    tc = TempCtrlClient(
        transport,
        settings={
            "watchdog_timeout_ms": 25000,
            "LNA": {
                "enable": True,
                "cooling_enabled": False,
                "target_C": 25.0,
                "clamp": 0.5,
                "Kp": 0.3,
                "Ki": 0.01,
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
    # Order: watchdog → clamp → cooling_enabled → gains → temperature → enable.
    assert sent == [
        "set_watchdog_timeout",
        "set_clamp",
        "set_cooling_enabled",
        "set_gains",
        "set_temperature",
        "set_enable",
    ]
