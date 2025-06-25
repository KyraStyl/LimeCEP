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
    symbol_id = random.randint(0, event_types - 1)
    symbol = symbol_id if system == 'sase' else chr(97 + symbol_id)
    price = random.randint(10, 100)
    volume = random.randint(1, 1000)

    if system == 'sase':
        # Comma-separated: id,time,symbol,price,volume
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
    ordered_events = []

    for i in range(event_count):
        ts = base_time + timedelta(minutes=i)
        logical_time = i + 1  # SASE discrete time
        event = generate_event(i, event_types, ts, system, logical_time)

        if isinstance(event, dict) and random.random() < delay_prob:
            delay = random.choices([1, 2, 3, 5, 10], weights=[30, 20, 10, 5, 2])[0]
            insert_pos = min(len(stream), random.randint(delay, len(stream)))
            event["out_of_order"] = True
            stream.insert(insert_pos, event)
        else:
            stream.append(event)

    return stream

def write_stream(stream, output_path, system):
    with open(output_path, 'w') as f:
        for event in stream:
            if system == 'sase':
                f.write(event + '\n')
            else:  # limecep
                f.write(json.dumps(event) + '\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic event stream.")
    parser.add_argument('--output', '-o', default='events.out')
    parser.add_argument('--start', '-s', required=True, help="Start datetime (e.g., 2025-02-01T10:00:00)")
    parser.add_argument('--system', choices=['sase', 'limecep'], required=True)
    parser.add_argument('--types', type=int, default=3, help="Number of event types")
    parser.add_argument('--count', type=int, default=100, help="Number of events to generate")
    parser.add_argument('--prob', type=float, default=0.1, help="Probability of out-of-order event")

    args = parser.parse_args()
    events = generate_stream(args.count, args.types, args.start, args.system, args.prob)
    write_stream(events, args.output, args.system)
