#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 15 02:50:02 2024

@author: kyrastyl
"""

import random
import json
import argparse
from datetime import datetime, timedelta

def create_fitbit_message(generation_time):
    timestamp = generation_time.strftime('%Y-%m-%dT%H:%M:%S')
    steps = random.randint(100, 20000) * 1.0
    stairs = random.randint(1, 15) * 1.0
    naps = random.randint(1, 5) * 1.0
    message = {"fitbit": {"timestamp": timestamp, "steps": steps, "stairs": stairs, "naps": naps}}
    return message

def create_scale_message(generation_time):
    timestamp = generation_time.strftime('%Y-%m-%dT%H:%M:%S')
    weight = random.uniform(68.0, 77.0)
    height = 175.0
    bmi = random.uniform(22.2, 22.9)
    bmiCat = "overweight"
    message = {"scale": {"timestamp": timestamp, "weight": weight, "height": height, "bmi": bmi, "bmiCategory": bmiCat}}
    return message

def create_locs_message(generation_time, send_rate):
    timestamp = generation_time.strftime('%Y-%m-%dT%H:%M:%S')
    loc = "Bedroom"
    dateEntered = (generation_time - timedelta(seconds=send_rate / 2)).strftime('%Y-%m-%dT%H:%M:%S')
    dateUpdate = timestamp
    totalActivations = random.randint(1, 10)
    recentActivations = random.randint(1, totalActivations)
    message = {"location": loc, "timestamp": timestamp, "dateEntered": dateEntered, "dateUpdate": dateUpdate, "totalActivations": totalActivations, "recentActivations": recentActivations}
    return message

def generate_timestamps(start_time, interval, count):
    return [start_time + i * interval for i in range(count)]

def shuffle_out_of_order(messages, probability):
    n = len(messages)
    num_out_of_order = int(n * probability)
    indices = list(range(n))
    out_of_order_indices = random.sample(indices, num_out_of_order)
    
    for i in out_of_order_indices:
        swap_with = random.choice([j for j in indices if j not in out_of_order_indices or j != i])
        messages[i], messages[swap_with] = messages[swap_with], messages[i]

    return messages

def introduce_significant_delays(messages, probability, min_delay_seconds, max_delay_seconds):
    delayed_messages = []
    for generation_time, label, create_func, send_rate in messages:
        if random.random() < probability:
            delay = timedelta(seconds=random.randint(min_delay_seconds, max_delay_seconds))
            ingestion_time = generation_time + delay
            delayed_messages.append((generation_time, ingestion_time, label, create_func, send_rate))
        else:
            delayed_messages.append((generation_time, generation_time, label, create_func, send_rate))
    return delayed_messages

def generate_events(start_time, duration_minutes, total_events, out_of_order_probability, min_delay_seconds, max_delay_seconds):
    fitbit_interval = timedelta(minutes=1)
    scale_interval = timedelta(minutes=20)
    locs_interval = timedelta(minutes=5)
    
    fitbit_count = int(total_events * 0.78)  # 60% of events
    scale_count = int(total_events * 0.05)  # 10% of events
    locs_count = total_events - fitbit_count - scale_count  # Remaining 30% of events
    
    fitbit_timestamps = generate_timestamps(start_time, fitbit_interval, fitbit_count)
    scale_timestamps = generate_timestamps(start_time, scale_interval, scale_count)
    locs_timestamps = generate_timestamps(start_time, locs_interval, locs_count)
    
    fitbit_messages = [(ts, "Fitbit", create_fitbit_message, None) for ts in fitbit_timestamps]
    scale_messages = [(ts, "Scale", create_scale_message, None) for ts in scale_timestamps]
    locs_messages = [(ts, "Location", create_locs_message, locs_interval.total_seconds()) for ts in locs_timestamps]

    all_messages = fitbit_messages + scale_messages + locs_messages

    # Shuffle messages out-of-order
    all_messages = shuffle_out_of_order(all_messages, out_of_order_probability)

    # Introduce significant delays
    all_messages = introduce_significant_delays(all_messages, out_of_order_probability, min_delay_seconds, max_delay_seconds)

    # Sort by ingestion time
    all_messages.sort(key=lambda x: x[1])

    return all_messages

def main():
    parser = argparse.ArgumentParser(description="This is a script to generate sensor data.")
    parser.add_argument('-n', '--events', type=int, required=True, help="Number of events to be created")
    parser.add_argument('-o', '--output', type=str, required=True, help="Name of the output file")
    parser.add_argument('-d', '--duration', type=int, default=60, help="Duration of events in minutes")
    parser.add_argument('--mind', type=int, default=120, help="Minimum delay for out-of-order messages in seconds")
    parser.add_argument('--maxd', type=int, default=240, help="Maximum delay for out-of-order messages in seconds")
    parser.add_argument('--prob', type=float, default=0.3, help="Probability of out-of-order messages")
    parser.add_argument('-s', '--start', type=str, required=True, help="Start time in format yyyy-MM-ddTHH:mm:ss")

    args = parser.parse_args()
    
    out_of_order_probability = args.prob / 100 if args.prob > 1 else args.prob

    start_time = datetime.strptime(args.start, '%Y-%m-%dT%H:%M:%S')
    messages = generate_events(start_time, args.duration, args.events, args.prob, args.mind, args.maxd)

    with open(args.output, 'w') as file:
        for generation_time, ingestion_time, label, create_func, send_rate in messages:
            message = create_func(generation_time, send_rate) if label == "Location" else create_func(generation_time)
            json.dump(message, file)
            file.write('\n')
        file.write('{"Terminate":true, "message":"terminate process"}')

if __name__ == "__main__":
    main()
