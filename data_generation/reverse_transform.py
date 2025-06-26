#!/usr/bin/env python3

import json
import argparse

SYMBOL_REVERSE_MAP = {'a': 0, 'b': 1, 'c': 2}

def reverse_transform(input_path, output_path):
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        for line in infile:
            event = json.loads(line)
            symbol_num = SYMBOL_REVERSE_MAP.get(event['symbol'], -1)
            if symbol_num == -1:
                raise ValueError(f"Unknown symbol: {event['symbol']}")
            output_line = f"{event['id']},{event['id']},{symbol_num},{event['price']},{event['volume']}\n"
            outfile.write(output_line)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconstruct original dataset.stream from JSON format.")
    parser.add_argument('-i', '--input', type=str, required=True, help='Input JSON lines file')
    parser.add_argument('-o', '--output', type=str, default='reconstructed.stream', help='Output reconstructed file')
    args = parser.parse_args()
    
    reverse_transform(args.input, args.output)
