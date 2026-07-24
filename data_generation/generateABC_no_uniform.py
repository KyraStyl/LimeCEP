#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import random
from datetime import datetime, timedelta

def generate_symbol_map(event_types):
    # SASE: numeric symbols, LIMECEP: alphabetic
    return {i: chr(97 + i) for i in range(event_types)}

def generate_event(event_id, event_types, timestamp, system, logical_time):

    # Non-uniform distribution (later symbols more frequent)
    weights = [(i + 1) ** 2 for i in range(event_types)]
    symbol_id = random.choices(range(event_types), weights=weights)[0]

    symbol = symbol_id if system == 'sase' else chr(97 + symbol_id)

    price = random.randint(10, 100)
    volume = random.randint(1, 1000)

    if system == 'sase':
        return f"{event_id},{logical_time},{symbol},{price},{volume}"
    else:  # LIMECEP
        return {
            "id": event_id,
            "timestamp": timestamp.strftime('%Y-%m-%dT%H:%M:%S'),
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "out_of_order": False
        }

def generate_stream(event_count, event_types, start_time, system, delay_prob):
    base_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
    stream = []

    for i in range(event_count):

        # events progress in order
        ts = base_time + timedelta(minutes=i)

        logical_time = i + 1
        event = generate_event(i, event_types, ts, system, logical_time)

        # Always append -> no out-of-order events
        stream.append(event)

    return stream

def write_stream(stream, output_path, system):
    with open(output_path, 'w') as f:
        for event in stream:
            if system == 'sase':
                f.write(event + '\n')
            else:  # limecep
                f.write(json.dumps(event) + '\n')
        f.write("{\"Terminate\":true, \"message\":\"terminate process\"}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic event stream.")
    parser.add_argument('--output', '-o', default='events.stream')
    parser.add_argument('--start', '-s', required=True, help="Start datetime (e.g., 2025-02-01T10:00:00)")
    parser.add_argument('--system', choices=['sase', 'limecep'], required=True)
    parser.add_argument('--types', type=int, default=3, help="Number of event types")
    parser.add_argument('--count', type=int, default=100, help="Number of events to generate")

    args = parser.parse_args()

    events = generate_stream(args.count, args.types, args.start, args.system, 0)

    write_stream(events, args.output, args.system)
