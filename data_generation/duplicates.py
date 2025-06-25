#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to duplicate events randomly in a dataset.
"""

import argparse
import json
import random
from datetime import datetime


def duplicate_events(input_file, output_file, duplication_probability, seed=None):
    if seed is not None:
        random.seed(seed)

    with open(input_file, 'r') as f:
        lines = f.readlines()

    events = [json.loads(line) for line in lines if line.strip()]
    output_events = []

    for event in events:
        output_events.append(event)
        if random.random() < duplication_probability:
            duplicate = event.copy()
            duplicate["id"] = f"{event['id']}"
            output_events.append(duplicate)

    with open(output_file, 'w') as f_out:
        for ev in output_events:
            json.dump(ev, f_out)
            f_out.write('\n')


def main():
    parser = argparse.ArgumentParser(description="Script to introduce duplicate events into an event stream.")
    parser.add_argument('-i', '--input', required=True, help="Input file (line-separated JSON events)")
    parser.add_argument('-o', '--output', required=True, help="Output file with duplicates added")
    parser.add_argument('-p', '--prob', type=float, default=0.1, help="Probability (0-1) of duplicating an event")
    parser.add_argument('--seed', type=int, default=None, help="Random seed for reproducibility")

    args = parser.parse_args()
    duplicate_events(args.input, args.output, args.prob, args.seed)


if __name__ == "__main__":
    main()

