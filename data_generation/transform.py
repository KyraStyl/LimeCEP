#!/usr/bin/env python3

import json
import argparse
import random
from datetime import datetime, timedelta

SYMBOL_MAP = {0: 'a', 1: 'b', 2: 'c'}

def parse_line(line):
    parts = list(map(int, line.strip().split(',')))
    return {
        "id": parts[0],
        "symbol": SYMBOL_MAP.get(parts[2], 'unknown'),
        "price": parts[3],
        "volume": parts[4]
    }

def transform_file(input_path, output_path, start_time, delay_prob):
    with open(input_path, 'r') as infile:
        lines = infile.readlines()

    base_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
    
    # Assign logical timestamps
    events = []
    for i, line in enumerate(lines):
        event = parse_line(line)
        event_time = base_time + timedelta(minutes=i)
        event['timestamp'] = event_time
        event['out_of_order'] = False
        events.append(event)

    # Simulate out-of-order emission
    stream = []
    for event in events:
        if random.random() < delay_prob:
            delay = random.choices(
                population=[1, 2, 3, 5, 10, 20, 60],
                weights=[30, 20, 10, 5, 3, 2, 1],
                k=1
            )[0]
            insert_pos = min(len(stream), random.randint(delay, len(stream) + delay))
            stream.insert(insert_pos, {**event, 'out_of_order': True})
        else:
            stream.append(event)

    # Write to output
    with open(output_path, 'w') as out:
        for event in stream:
            output = {
                "id": event["id"],
                "timestamp": event["timestamp"].strftime('%Y-%m-%dT%H:%M:%S'),
                "symbol": event["symbol"],
                "price": event["price"],
                "volume": event["volume"],
                "out_of_order": event["out_of_order"]
            }
            out.write(json.dumps(output) + '\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform dataset.stream with out-of-order simulation.")
    parser.add_argument('--input', '-i', default='dataset.stream')
    parser.add_argument('--output', '-o', default='transformed.json')
    parser.add_argument('--start', '-s', required=True, help="Start datetime (e.g., 2025-02-01T10:00:00)")
    parser.add_argument('--prob', type=float, default=0.1, help="Probability of out-of-order event")

    args = parser.parse_args()
    transform_file(args.input, args.output, args.start, args.prob)
