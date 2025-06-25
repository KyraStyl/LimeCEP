#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 15 04:32:33 2024

@author: kyrastyl
"""

import argparse
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

def delete_topics(admin_client, topics):
    """Delete Kafka topics."""
    try:
        admin_client.delete_topics(topics=topics)
        print(f"Topics {topics} deleted successfully.")
    except Exception as e:
        print(f"Failed to delete topics {topics}: {e}")

def create_topics(admin_client, topics):
    """Create Kafka topics if they do not exist."""
    existing_topics = admin_client.list_topics()
    topics_to_create = [NewTopic(name=topic, num_partitions=1, replication_factor=1) for topic in topics if topic not in existing_topics]

    if topics_to_create:
        try:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print(f"Topics {topics} created successfully.")
        except Exception as e:
            print(f"Failed to create topics {topics}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Script to clear Kafka topics.")
    parser.add_argument('-s', '--server', type=str, default="localhost:9092", required=False, help="Kafka server address")
    parser.add_argument('-t', '--topics', nargs='+', required=True, help="List of topics to clear")

    args = parser.parse_args()

    admin_client = KafkaAdminClient(bootstrap_servers=args.server)

    # Delete and recreate topics
    delete_topics(admin_client, args.topics)
    create_topics(admin_client, args.topics)

    admin_client.close()

if __name__ == '__main__':
    main()