"""Live ADC snapshot plotter. Connects directly to the SNAP board
via casperfpga — no Redis, no eigsep_corr dependency.
Optionally saves snapshots to an npz file."""

import argparse
import struct
import time

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import casperfpga
from casperfpga.transport_tapcp import TapcpTransport

SAMPLES_PER_POL = 2048


def get_adc_snapshot(fpga, antenna):
    """Read ADC snapshot for both pols of an antenna.

    Returns (pol_x, pol_y) as numpy arrays of signed 8-bit samples.
    """
    devs = fpga.listdev()
    if "input_snap_sel" in devs:
        fpga.write_int("input_snap_sel", antenna)
        fpga.write_int("input_snapshot_ctrl", 0)
        fpga.write_int("input_snapshot_ctrl", 1)
        fpga.write_int("input_snapshot_ctrl", 3)
        d = struct.unpack(
            ">%db" % (2 * SAMPLES_PER_POL),
            fpga.read("input_snapshot_bram", 2 * SAMPLES_PER_POL),
        )
        x, y = [], []
        for i in range(SAMPLES_PER_POL // 2):
            x += [d[4 * i], d[4 * i + 1]]
            y += [d[4 * i + 2], d[4 * i + 3]]
    else:
        fpga.write_int("input_snapshot_ctrl", 0)
        fpga.write_int("input_snapshot_ctrl", 1)
        fpga.write_int("input_snapshot_ctrl", 3)
        nbytes = 16 * SAMPLES_PER_POL // 2
        d = struct.unpack(
            ">%db" % nbytes,
            fpga.read("input_snapshot_bram", nbytes),
        )
        x, y = [], []
        for i in range(SAMPLES_PER_POL // 2):
            # antenna + 1 because antenna 0 is a dummy (all zeros)
            off = 4 * (antenna + 1)
            x += [d[16 * i + off], d[16 * i + off + 1]]
            y += [d[16 * i + off + 2], d[16 * i + off + 3]]
    return np.array(x), np.array(y)


def get_power_spectra(fpga, antenna, acc_len=1):
    """Software FFT power spectrum accumulated over acc_len snapshots."""
    X = np.zeros(SAMPLES_PER_POL // 2 + 1)
    Y = np.zeros(SAMPLES_PER_POL // 2 + 1)
    for _ in range(acc_len):
        x, y = get_adc_snapshot(fpga, antenna)
        X += np.abs(np.fft.rfft(x)) ** 2
        Y += np.abs(np.fft.rfft(y)) ** 2
    return X, Y


parser = argparse.ArgumentParser(
    description="Live ADC snapshot plotter (casperfpga only)",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "snap_ip",
    help="SNAP board IP address",
)
parser.add_argument(
    "--antenna",
    type=int,
    default=0,
    help="Antenna index to plot",
)
parser.add_argument(
    "--power-spectrum",
    action="store_true",
    help="Plot power spectrum instead of time-domain samples",
)
parser.add_argument(
    "--acc-len",
    type=int,
    default=1,
    help="Number of spectra to average (only with --power-spectrum)",
)
parser.add_argument(
    "--interval",
    type=int,
    default=200,
    help="Update interval in milliseconds",
)
parser.add_argument(
    "--save",
    type=str,
    default=None,
    metavar="FILE",
    help="Save snapshots to npz file on exit",
)
parser.add_argument(
    "--nsamples",
    type=int,
    default=None,
    help="Number of snapshots to save, then exit. Requires --save.",
)
args = parser.parse_args()

# Connect to SNAP
print(f"Connecting to SNAP at {args.snap_ip}...")
fpga = casperfpga.CasperFpga(args.snap_ip, transport=TapcpTransport)
print(f"Connected. Firmware: {fpga.listdev()[:3]}...")

ant = args.antenna
snapshots = []
snapshot_count = 0


def capture():
    if args.power_spectrum:
        return get_power_spectra(fpga, ant, acc_len=args.acc_len)
    return get_adc_snapshot(fpga, ant)


# Initial capture to set up axes
px, py = capture()
fig, axes = plt.subplots(2, 1, figsize=(10, 6))

if args.power_spectrum:
    xaxis = np.arange(len(px))
    (line_x,) = axes[0].semilogy(xaxis, px)
    (line_y,) = axes[1].semilogy(xaxis, py)
    axes[1].set_xlabel("Channel")
    for ax in axes:
        ax.set_ylabel("Power")
        ax.grid(True)
else:
    xaxis = np.arange(len(px))
    (line_x,) = axes[0].plot(xaxis, px)
    (line_y,) = axes[1].plot(xaxis, py)
    axes[1].set_xlabel("Sample")
    for ax in axes:
        ax.set_ylim(-128, 128)
        ax.set_ylabel("ADC counts")
        ax.grid(True)

axes[0].set_title(f"Antenna {ant} — Pol X")
axes[1].set_title(f"Antenna {ant} — Pol Y")
plt.tight_layout()


def update(frame):
    global snapshot_count
    px, py = capture()
    line_x.set_ydata(px)
    line_y.set_ydata(py)
    if args.power_spectrum:
        for ax in axes:
            ax.relim()
            ax.autoscale_view()
    if args.save:
        snapshots.append(
            {"time": time.time(), "pol_x": px, "pol_y": py}
        )
        snapshot_count += 1
        if args.nsamples and snapshot_count >= args.nsamples:
            ani.event_source.stop()
            plt.close(fig)
    return line_x, line_y


ani = FuncAnimation(
    fig, update, interval=args.interval, blit=False,
    cache_frame_data=False,
)

try:
    plt.show()
except KeyboardInterrupt:
    print("\nStopping.")
finally:
    plt.close(fig)
    if args.save and snapshots:
        times = np.array([s["time"] for s in snapshots])
        pol_x = np.array([s["pol_x"] for s in snapshots])
        pol_y = np.array([s["pol_y"] for s in snapshots])
        np.savez(
            args.save,
            times=times,
            pol_x=pol_x,
            pol_y=pol_y,
            antenna=ant,
        )
        print(f"Saved {len(snapshots)} snapshots to {args.save}.npz")
