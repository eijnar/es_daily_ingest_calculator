from datetime import datetime, timedelta
import os
import time
import csv
import urllib3

from elasticsearch import Elasticsearch, exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')
ELASTICSEARCH_APIKEY = os.getenv('ELASTICSEARCH_APIKEY')
OUTPUT_FILE = 'daily_ingest_report.csv'


es = Elasticsearch(ELASTICSEARCH_HOST,
                   api_key=ELASTICSEARCH_APIKEY, verify_certs=False)


def get_all_indices():
    try:
        indices = es.cat.indices(format="json", h='index')
        return [index['index'] for index in indices]
    except exceptions.BadRequestError as e:
        print(f"Error fetching indices: {e}")
        return []


def is_index_active_today(index):
    today = datetime.now().date()
    query = {
        "size": 1,
        "query": {
            "range": {
                "@timestamp": {
                    "gte": today.isoformat(),
                    "lt": (today + timedelta(days=1)).isoformat()
                }
            }
        }
    }
    try:
        response = es.search(index=index, body=query)
        return response['hits']['total']['value'] > 0
    except exceptions.BadRequestError:
        return False


def calculate_daily_ingest(size_in_bytes, first_timestamp, last_timestamp):
    try:
        first_time = datetime.fromisoformat(
            first_timestamp.replace('Z', '+00:00'))
        last_time = datetime.fromisoformat(
            last_timestamp.replace('Z', '+00:00'))
        duration_hours = (last_time - first_time).total_seconds() / 3600
        if duration_hours == 0:
            return 0
        daily_ingest_mb = (size_in_bytes / (1024 * 1024)) * \
            (24 / duration_hours)
        return round(daily_ingest_mb, 2)
    except Exception as e:
        print(f"Error calculating daily ingest: {e}")
        return 0


def get_index_stats(index):
    try:
        stats = es.indices.stats(index=index, level='shards', metric='store')
        size_in_bytes = stats['indices'][index]['primaries']['store']['size_in_bytes']

        # Get first and last document timestamps
        first_doc = es.search(index=index, size=1, sort="@timestamp:asc")
        last_doc = es.search(index=index, size=1, sort="@timestamp:desc")

        first_timestamp = first_doc['hits']['hits'][0]['_source'].get(
            '@timestamp', 'N/A')
        last_timestamp = last_doc['hits']['hits'][0]['_source'].get(
            '@timestamp', 'N/A')

        daily_ingest_mb = calculate_daily_ingest(
            size_in_bytes, first_timestamp, last_timestamp)

        return {
            'index': index,
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'daily_ingest_mb': daily_ingest_mb
        }

    except exceptions.BadRequestError as e:
        print(f"Error fetching stats for index {index}: {e}")
        return None


def initialize_csv(filename=OUTPUT_FILE):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            'index', 'first_timestamp', 'last_timestamp', 'daily_ingest_mb'
        ])
        writer.writeheader()


def append_to_csv(data, filename=OUTPUT_FILE):
    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            'index', 'first_timestamp', 'last_timestamp', 'daily_ingest_mb'
        ])
        writer.writerow(data)


def main():
    print("Fetching all indices...")
    indices = get_all_indices()

    active_indices = []
    print("Checking which indices have data from today...")
    for index in indices:
        print(f"Checking index: {index}")
        if is_index_active_today(index):
            active_indices.append(index)

    print(f"Found {len(active_indices)} active indices.")

    initialize_csv()
    for index in active_indices:
        print(f"Gathering stats for index: {index}")
        stats = get_index_stats(index)
        if stats:
            append_to_csv(stats)
        time.sleep(0.1)  # To avoid overwhelming Elasticsearch


if __name__ == "__main__":
    main()
