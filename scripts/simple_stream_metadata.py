import json
import time
import argparse
import h5py
import numpy as np
import signal
import atexit
from pathlib import Path
from datetime import datetime
from eigsep_observing import EigsepRedis

"""
Records metadata from redis for the purposes of in-lab testing. 

Author: Richard Saeed

Parser:
--------
--output: Specify metadata save directory.
--format: Default 'json', can change to "both" or "hdf5"'
--interval: Default 1.0s interval between saves. 
--keys: Default ["tempctrl", "tempctrl_ts"] keys

Output:
--------
JSON file: metadata_{timestamp}.json

"""
def save_to_json(data_buffer, filepath):
    """Save collected metadata to a JSON file."""
    with open(filepath, 'w') as f:
        json.dump(data_buffer, f, indent=2)
    print(f"Saved {len(data_buffer)} records to {filepath}")


def save_to_hdf5(data_buffer, filepath):
    """Save collected metadata to an HDF5 file."""
    with h5py.File(filepath, 'w') as f:
        # Save metadata
        f.attrs['n_records'] = len(data_buffer)
        f.attrs['created'] = datetime.now().isoformat()

        # Extract all unique keys from all records
        all_keys = set()
        for record in data_buffer:
            all_keys.update(record.keys())

        # Create datasets for each key
        for key in all_keys:
            # Collect all values for this key
            values = []
            for record in data_buffer:
                if key in record:
                    values.append(record[key])
                else:
                    values.append(None)

            # Save based on data type
            if key.endswith('_ts'):
                # Timestamps - save as float64
                f.create_dataset(key, data=np.array(values, dtype=np.float64))
            elif all(isinstance(v, (int, float)) or v is None for v in values):
                # Numeric data
                # Replace None with NaN for numeric data
                numeric_values = [float(v) if v is not None else np.nan for v in values]
                f.create_dataset(key, data=np.array(numeric_values, dtype=np.float64))
            elif all(isinstance(v, dict) or v is None for v in values):
                # Nested dictionaries - save as JSON strings
                json_strings = [json.dumps(v) if v is not None else "" for v in values]
                dt = h5py.string_dtype(encoding='utf-8')
                f.create_dataset(key, data=np.array(json_strings, dtype=dt))
            else:
                # Other data - convert to strings
                str_values = [str(v) for v in values]
                dt = h5py.string_dtype(encoding='utf-8')
                f.create_dataset(key, data=np.array(str_values, dtype=dt))

    print(f"Saved {len(data_buffer)} records to {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Stream and save metadata from Panda')
    parser.add_argument('--output', '-o', type=str,
                        help='Output filename (without extension)')
    parser.add_argument('--format', '-f', type=str,
                        choices=['json', 'hdf5', 'both'],
                        default='json',
                        help='Output format (default: json)')
    parser.add_argument('--interval', '-i', type=float,
                        default=1.0,
                        help='Sampling interval in seconds (default: 1.0)')
    parser.add_argument('--keys', '-k', type=str, nargs='+',
                        default=["tempctrl", "tempctrl_ts"],
                        help='Metadata keys to retrieve')
    parser.add_argument('--host', type=str,
                        default="10.10.10.11",
                        help='Redis host address')
    parser.add_argument('--autosave-interval', type=int,
                        default=0,
                        help='Auto-save every N records (default: 100, set to 0 to disable)')

    args = parser.parse_args()

    # Generate default filename if not provided
    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"metadata_{timestamp}"

    r = EigsepRedis(args.host)
    data_buffer = []
    last_saved_count = 0

    def save_data(reason=""):
        """Save data to file(s) based on format setting."""
        nonlocal last_saved_count

        if len(data_buffer) > 0 and len(data_buffer) > last_saved_count:
            print("\n" + "-" * 50)
            if reason:
                print(f"Saving data ({reason})... Collected {len(data_buffer)} records")
            else:
                print(f"Saving data... Collected {len(data_buffer)} records")

            # Save to requested format(s)
            if args.format in ['json', 'both']:
                json_path = Path(args.output).with_suffix('.json')
                save_to_json(data_buffer, json_path)

            if args.format in ['hdf5', 'both']:
                hdf5_path = Path(args.output).with_suffix('.hdf5')
                save_to_hdf5(data_buffer, hdf5_path)

            last_saved_count = len(data_buffer)
            print("-" * 50)
        elif len(data_buffer) == 0:
            print("\nNo data collected, nothing to save.")

    def signal_handler(signum, frame):
        """Handle interrupt signals by saving data."""
        signal_name = signal.Signals(signum).name
        save_data(f"received {signal_name}")
        exit(0)

    # Register signal handlers for various interrupts
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal
    try:
        signal.signal(signal.SIGHUP, signal_handler)   # Hangup (terminal closed)
    except AttributeError:
        # SIGHUP not available on Windows
        pass

    # Register atexit handler as a fallback
    atexit.register(lambda: save_data("program exit"))

    print(f"Streaming metadata from {args.host}")
    print(f"Keys: {args.keys}")
    print(f"Interval: {args.interval}s")
    if args.autosave_interval > 0:
        print(f"Auto-save: every {args.autosave_interval} records")
    print("Press Ctrl+C to stop and save...")
    print("-" * 50)

    try:
        while True:
            m = r.get_live_metadata(keys=args.keys)

            # Add timestamp for this record
            m['_record_time'] = time.time()

            data_buffer.append(m)
            print(json.dumps(m, indent=2, sort_keys=False))

            # Auto-save periodically
            if args.autosave_interval > 0 and len(data_buffer) % args.autosave_interval == 0:
                save_data(f"auto-save at {len(data_buffer)} records")

            time.sleep(args.interval)

    except KeyboardInterrupt:
        save_data("keyboard interrupt")
    except Exception as e:
        print(f"\nError occurred: {e}")
        save_data("error/exception")


if __name__ == '__main__':
    main()
