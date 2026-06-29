"""Serial-less PicoMotor for step<->degree conversion.

PicoManager builds the real motor pico with PicoMotor's constructor
defaults and never overrides step_angle_deg / gear_teeth / microstep,
so reusing those defaults here makes host-side degree math match the
mover's own deg_to_steps exactly. The __new__ bypass avoids serial I/O.
"""

import inspect

from picohost.motor import PicoMotor


def cal_motor():
    sig = inspect.signature(PicoMotor.__init__)
    cal = PicoMotor.__new__(PicoMotor)
    for attr in ("step_angle_deg", "gear_teeth", "microstep"):
        setattr(cal, attr, sig.parameters[attr].default)
    return cal
