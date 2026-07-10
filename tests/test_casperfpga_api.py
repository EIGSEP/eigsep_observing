"""Consumer-contract test for the pinned ``casperfpga`` fork.

This file documents — and enforces in CI — the exact ``casperfpga`` API
surface that ``eigsep_observing`` depends on. The fork is pinned in
``hardware-requirements.txt`` to a tag of ``github.com/EIGSEP/casperfpga``;
this test runs only in the ``test-with-casperfpga`` CI job (it skips
silently elsewhere via ``importorskip``) and fails loudly the moment the
fork drops or renames a symbol we use.

Add an assertion here whenever new code starts calling a ``casperfpga``
attribute that isn't already covered.
"""

import pytest

casperfpga = pytest.importorskip("casperfpga")


def test_top_level_classes_importable():
    from casperfpga import snapadc
    from casperfpga.transport_tapcp import TapcpTransport

    assert hasattr(casperfpga, "CasperFpga")
    assert hasattr(snapadc, "SnapAdc")
    assert TapcpTransport is not None


def test_i2c_submodules_importable():
    # All eight submodules ship in our fork tag. Five are used directly
    # by ``blocks.Pam``; the remaining three (i2c_bar / i2c_motion /
    # i2c_temp) were used by ``blocks.Fem``, which has since been
    # removed from blocks.py. The full set is still asserted as a
    # fork-availability smoke check — see ``test_fem_i2c_class_surface``.
    from casperfpga import (  # noqa: F401
        i2c,
        i2c_bar,
        i2c_eeprom,
        i2c_gpio,
        i2c_motion,
        i2c_sn,
        i2c_temp,
        i2c_volt,
    )


def test_casperfpga_register_methods():
    # Methods called on ``CasperFpga`` instances by ``blocks.Block`` and
    # ``EigsepFpga`` (see fpga.py and blocks.py).
    cls = casperfpga.CasperFpga
    for method in (
        "read_int",
        "read_uint",
        "write_int",
        "read",
        "write",
        "blindwrite",
        "listdev",
        "upload_to_ram_and_program",
    ):
        assert hasattr(cls, method), f"CasperFpga missing {method!r}"


def test_snapadc_methods():
    # Methods called on ``SnapAdc`` instances by ``EigsepFpga.initialize_adc``.
    from casperfpga.snapadc import SnapAdc

    for method in (
        "init",
        "alignLineClock",
        "alignFrameClock",
        "rampTest",
        "selectADC",
        "set_gain",
    ):
        assert hasattr(SnapAdc, method), f"SnapAdc missing {method!r}"


def test_snapadc_device_info_contract():
    # ``EigsepFpga._make_adc`` constructs SnapAdc with a device_info
    # dict whose keys (``adc_resolution``, ``sample_rate``,
    # ``snap_inputs``) are read inside ``__init__`` before any FPGA
    # I/O. If the fork renames any of those keys, SnapAdc raises
    # while reading the dict and the production call site breaks.
    # An empty dict triggers the same KeyError path, so we don't
    # need a working host stub here. ``ref=None`` (external-clock
    # path added in casperfpga 0.7.1) is also asserted — the
    # production caller passes ``ref=ref`` and a kwarg rename would
    # silently fall back to the default LMX path.
    from casperfpga.snapadc import SnapAdc

    with pytest.raises(KeyError):
        SnapAdc(host=None, device_name="snap_adc", device_info={})

    device_info = {
        "adc_resolution": 8,
        "sample_rate": 250e6,
        "snap_inputs": 2,
    }
    # With every required key supplied, ``__init__`` advances past
    # the device_info block and fails later on ``host=None`` (e.g.
    # WishBone helpers dereferencing ``host.host``). The exact
    # exception type isn't part of the contract; we only assert
    # that the failure is *not* a KeyError on a device_info field.
    with pytest.raises(Exception) as excinfo:
        SnapAdc(
            host=None,
            device_name="snap_adc",
            device_info=device_info,
            ref=None,
        )
    assert not isinstance(excinfo.value, KeyError), (
        "SnapAdc raised KeyError despite device_info supplying every "
        "required key — the dict contract changed"
    )


def test_pam_i2c_class_surface():
    # Symbols used by ``blocks.Pam`` to talk to the PAM I2C devices.
    from casperfpga import i2c, i2c_eeprom, i2c_gpio, i2c_sn, i2c_volt

    assert hasattr(i2c, "I2C")
    for method in ("enable_core", "setClock"):
        assert hasattr(i2c.I2C, method), f"i2c.I2C missing {method!r}"

    assert hasattr(i2c_gpio, "PCF8574")
    for method in ("read", "write"):
        assert hasattr(i2c_gpio.PCF8574, method), (
            f"i2c_gpio.PCF8574 missing {method!r}"
        )

    for cls_name in ("INA219", "MAX11644"):
        assert hasattr(i2c_volt, cls_name), f"i2c_volt missing {cls_name!r}"
        cls = getattr(i2c_volt, cls_name)
        for method in ("init", "readVolt"):
            assert hasattr(cls, method), (
                f"i2c_volt.{cls_name} missing {method!r}"
            )

    assert hasattr(i2c_eeprom, "EEP24XX64")
    for method in ("readString", "writeString"):
        assert hasattr(i2c_eeprom.EEP24XX64, method), (
            f"i2c_eeprom.EEP24XX64 missing {method!r}"
        )

    assert hasattr(i2c_sn, "DS28CM00")
    assert hasattr(i2c_sn.DS28CM00, "readSN")


def test_fem_i2c_class_surface():
    # Symbols formerly used by ``blocks.Fem`` to talk to the FEM I2C
    # devices. ``Fem`` has been removed from blocks.py, so no in-tree
    # code currently calls these — the assertions remain as a
    # hardware-availability smoke check that the fork retains the FEM
    # classes, in case ``Fem`` (or an equivalent block) is reintroduced.
    from casperfpga import i2c_bar, i2c_motion, i2c_temp

    assert hasattr(i2c_bar, "MS5611_01B")
    for method in ("init", "readTemp", "readPress"):
        assert hasattr(i2c_bar.MS5611_01B, method), (
            f"i2c_bar.MS5611_01B missing {method!r}"
        )

    assert hasattr(i2c_motion, "IMUSimple")
    assert hasattr(i2c_motion.IMUSimple, "init")
    assert hasattr(i2c_motion.IMUSimple, "pose")

    for cls_name, methods in (
        ("Si7051", ("readTemp", "sn")),
        ("Si7021", ("readTempRH", "model")),
    ):
        assert hasattr(i2c_temp, cls_name), f"i2c_temp missing {cls_name!r}"
        cls = getattr(i2c_temp, cls_name)
        for method in methods:
            assert hasattr(cls, method), (
                f"i2c_temp.{cls_name} missing {method!r}"
            )


def test_casperfpga_constructor_uses_transport_kwarg():
    # ``EigsepFpga._make_fpga`` calls ``CasperFpga(snap_ip, transport=...)``.
    # Upstream's signature is ``(self, *args, **kwargs)``, so signature
    # inspection can't see the ``transport`` kwarg — we have to exercise
    # it. A recording stub raises after capturing kwargs so the rest of
    # the constructor (which talks to real hardware) doesn't run. If the
    # fork ever renames or drops the ``transport`` kwarg, the stub
    # never gets called and the assertion fails.
    instances = []

    class RecorderTransport:
        def __init__(self, **kwargs):
            instances.append(kwargs)
            raise RuntimeError("stop after transport ctor")

    with pytest.raises(RuntimeError, match="stop after transport ctor"):
        casperfpga.CasperFpga("dummyhost", transport=RecorderTransport)

    assert instances, "CasperFpga.__init__ did not honor transport=... kwarg"


def test_casperfpga_forwards_timeout_kwarg_to_transport():
    # ``EigsepFpga._make_fpga`` passes ``timeout=cfg["tapcp_timeout_s"]``
    # relying on ``CasperFpga.__init__`` forwarding its ``**kwargs`` to
    # the transport constructor (``transport(**kwargs)``), where
    # ``TapcpTransport`` reads ``kwargs.get('timeout', 3)`` — the 3 s
    # default being the per-lost-packet stall behind issue #204. Same
    # recording-stub pattern as the transport-kwarg test above; if the
    # fork ever stops forwarding constructor kwargs, ``timeout`` goes
    # missing here and the field silently reverts to 3 s stalls.
    instances = []

    class RecorderTransport:
        def __init__(self, **kwargs):
            instances.append(kwargs)
            raise RuntimeError("stop after transport ctor")

    with pytest.raises(RuntimeError, match="stop after transport ctor"):
        casperfpga.CasperFpga(
            "dummyhost", transport=RecorderTransport, timeout=0.25
        )

    assert instances, "CasperFpga.__init__ did not honor transport=... kwarg"
    assert instances[0].get("timeout") == 0.25, (
        "CasperFpga.__init__ did not forward timeout=... to the "
        "transport constructor"
    )
