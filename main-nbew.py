import os
import csv
import logging
import sys
from datetime import timezone, datetime, timedelta, time
import json

from elasticsearch import Elasticsearch
from tqdm import tqdm


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)

ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')
ELASTICSEARCH_APIKEY = os.getenv('ELASTICSEARCH_APIKEY')
OUTPUT_FILE = 'datastream_shard_stats.csv'


def log_message(message, level="info"):
    if level == "info":
        tqdm.write(f"{datetime.now(timezone.utc)} - INFO - {message}")
    elif level == "error":
        tqdm.write(f"{datetime.now(timezone.utc)} - ERROR - {message}")


def get_previous_day():
    """Return the start and end datetime objects from the previous day."""
    today = datetime.now(timezone.utc)
    start_of_day = datetime.combine(today - timedelta(days=), time.min)
    end_of_day = datetime.combine(today, time.min)
    log_message(f"start: {start_of_day.isoformat()}, end: {
                end_of_day.isoformat()}")
    return start_of_day, end_of_day


def classify_environment(datastream_name):
    """Classify the environment based on keywords in the datastream name."""
    datastream_name_lower = datastream_name.lower()
    if "nonprod" in datastream_name_lower:
        return "nonprod"
    if "prod" in datastream_name_lower:
        return "prod"
    if "dev" in datastream_name_lower:
        return "dev"
    if "default" in datastream_name_lower:
        return "default"
    if "operations" in datastream_name_lower:
        return "operations"
    else:
        return "other"


def get_all_indices(es_client):
    """Fetch all indices from the cluster."""
    try:
        indices = es_client.cat.indices(format='json')
        return [index['index'] for index in indices]
    except Exception as e:
        log_message(f"Error fetching all indices: {e}", level="error")
        return []


def get_indices_with_data(es_client, start_date, end_date):
    """Fetch indices that have data within the given date range."""
    indices_with_data = []
    problematic_indices = []
    all_indices = get_all_indices(es_client)

    log_message(f"Found {len(all_indices)} indices to check.")

    query = {
        "query": {
            "range": {
                "@timestamp": {
                    "gte": start_date.isoformat(),
                    "lt": end_date.isoformat()
                }
            }
        },
        "size": 1
    }

    for index in tqdm(all_indices, desc="Checking indices"):
        try:

            response = es_client.search(
                index=index, body=query, allow_no_indices=True)
            # logger.debug(f"Looking inside {index}")
            if response['hits']['total']['value'] > 0:
                # log_message(f"Appended: {index}")
                indices_with_data.append(index)
        except Exception as e:
            log_message(f"Error searching index '{index}': {e}", level="error")
            problematic_indices.append(index)

    if problematic_indices:
        log_message(f"\nSkipped problematic indices due to errors: {
                    problematic_indices}")

    return indices_with_data


def get_shard_stats(es_client):
    try:
        stats = es_client.indices.stats(level='shards')
        return stats['indices']
    except Exception as e:
        log_message(f"Error fetching shard stats {e}", level="error")
        return


def map_indices_to_datastreams(es_client):
    """Fetch data streams and map indices to their parent data streams."""
    try:
        datastreams = es_client.indices.get_data_stream()
        index_to_datastream = {}

        for ds in datastreams['data_streams']:
            ds_name = ds['name']
            for index in ds['indices']:
                index_to_datastream[index['index_name']] = ds_name
        return index_to_datastream

    except Exception as e:
        log_message(f"Error fetching data streams: {e}", level="error")
        return

def fetch_datastream_config(es_client, datastream_name, valid_indices):
    """Fetch datastream configurations, including primary/replica settings and ILM policy"""

    try: 
        log_message(f"Fetching config for datastream: {datastream_name}")
        datastream_details = es_client.indices.get_data_stream(name=datastream_name)
        log_message(f"Datastream details: {datastream_details}")
        datastreams = datastream_details.get('data_streams', [])

        template_name = datastreams[0].get('template', 'Unknown')
        log_message(f"Template name: {template_name}")

        ilm_policy_name = datastreams[0].get('ilm_policy', 'None')
        log_message(f"ILM policy name: {ilm_policy_name}")

        backing_indices = [index['index_name'] for index in datastreams[0].get('indices', [])]

        first_valid_index = next((index for index in backing_indices if index in valid_indices), None)

        if not first_valid_index:
            return {
                "primaries": "Unknown",
                "replicas": "Unknown",
                "ilm_policy": "None",
                "ilm_phases": {}
            }

        index_settings = es_client.indices.get_settings(index=first_valid_index)
        index_config = index_settings.get(first_valid_index, {}).get('settings', {}).get('index', {})

        number_of_primaries = index_config.get('number_of_shards', 'Unknown')
        number_of_replicas = index_config.get('number_of_replicas', 'Unknown')

        ilm_phases = {}
        if ilm_policy_name != 'None':
            try:
                ilm_details = es_client.ilm.get_lifecycle(name=ilm_policy_name)
                policy = ilm_details[ilm_policy_name]['policy']
                phases = policy.get('phases', {})
                for phase, phase_details in phases.items():
                    compact_json = json.dumps(phase_details, separators=(',', ':'))
                    ilm_phases[phase] = compact_json
            except Exception as e:
                log_message(f"Error fetching ILM policy '{ilm_policy_name}: {e}")

        return {
            "primaries": number_of_primaries,
            "replicas": number_of_replicas,
            "ilm_policy": ilm_policy_name,
            "ilm_phases": ilm_phases
        }
    except Exception as e:
        log_message(f"Error fetching datastream config for {datastream_name}: {e}", level="error")
        return {
            "primaries": "Unknown",
            "replicas": "Unknown",
            "ilm_policy": "None",
            "ilm_phases": {},
        }

def aggregate_shard_sizes(shard_stats, index_to_datastream, valid_indices):
    """Aggregate shard sizes by data stream."""
    datastream_sizes = {}

    for index, data in shard_stats.items():
        if index not in valid_indices:
            continue

        datastream = index_to_datastream.get(index, "Unknown")
        shards = data.get('shards', {})

        primary_size = 0
        replica_size = 0

        for shard_list in shards.values():
            for shard in shard_list:
                size = shard['store']['size_in_bytes']
                if shard['routing']['primary']:
                    primary_size += size
                else:
                    replica_size += size

        if datastream not in datastream_sizes:
            log_message(f"Initiating datastream entry for: {datastream}")
            datastream_sizes[datastream] = {
                "indices": [],
                "total_size": 0,
                "primary_size": 0,
                "replica_size": 0
            }

        if "indices" not in datastream_sizes[datastream] or "total_size" not in datastream_sizes[datastream]:
            log_message(f"Unexpected structure for datastream_sizes[{
                        datastream}]: {datastream_sizes[datastream]}")
            continue

        datastream_sizes[datastream]['indices'].append(index)
        datastream_sizes[datastream]['total_size'] += primary_size + replica_size
        datastream_sizes[datastream]['primary_size'] += primary_size
        datastream_sizes[datastream]['replica_size'] += replica_size

    return datastream_sizes


def write_to_csv(datastream_sizes, output_file, datastream_configs):
    """Write the aggregated data stream sizes to a CSV file."""
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "Datastream", 
            "Index", 
            "Total Size (Bytes)", 
            "Primary Size (Bytes)", 
            "Replica Size (Bytes)", 
            "Configured Primaries",
            "Configured Replicas",
            "ILM Policy",
            "Hot",
            "Warm",
            "Cold",
            "Frozen",
            "Delete",
            "Environment"
        ])
        for datastream, data in datastream_sizes.items():
            indices_list = ", ".join(data['indices'])
            total_size = data['total_size']
            primary_size = data['primary_size']
            replica_size = data['replica_size']

            config = datastream_configs.get(datastream, {})
            configured_primaries = config.get("primaries", "Unknown")
            configured_replicas = config.get("replicas", "Unknown")
            ilm_policy = config.get("ilm_policy", "None")
            ilm_phases = config.get("ilm_phases", "None")

            hot_actions = ilm_phases.get("hot", "")
            warm_actions = ilm_phases.get("warm", "")
            cold_actions = ilm_phases.get("cold", "")
            frozen_actions = ilm_phases.get("frozen", "")
            delete_actions = ilm_phases.get("delete", "")

            environment = classify_environment(datastream)
            writer.writerow([
                datastream, 
                indices_list,
                total_size,
                primary_size,
                replica_size,
                configured_primaries,
                configured_replicas,
                ilm_policy,
                hot_actions,
                warm_actions,
                cold_actions,
                frozen_actions,
                delete_actions,
                environment
            ])


def main():
    es_client = Elasticsearch(
        ELASTICSEARCH_HOST,
        api_key=ELASTICSEARCH_APIKEY
    )

    start_of_day, end_of_day = get_previous_day()

    valid_indices = get_indices_with_data(es_client, start_of_day, end_of_day)

    if not valid_indices:
        log_message("No indices found with data for the previous day.")

    shard_stats = get_shard_stats(es_client)

    if not shard_stats:
        log_message("No shard stats retrieved.", level="error")
        return

    log_message("Mapping indecies to datastreams")
    index_to_datastream = map_indices_to_datastreams(es_client)
    log_message("Mapping complete.")

    if not index_to_datastream:
        log_message("No datastreams found", level="error")

    log_message("Aggregating shard sizes")
    datastream_sizes = aggregate_shard_sizes(
        shard_stats, index_to_datastream, valid_indices)
    log_message("Aggregation complete")

    datastream_configs = {}
    for datastream in datastream_sizes.keys():
        datastream_configs[datastream] = fetch_datastream_config(es_client, datastream, valid_indices)

    log_message("Writing CSV file to disc")
    write_to_csv(datastream_sizes, OUTPUT_FILE, datastream_configs)

    log_message(f"Datastream shard stats written to {OUTPUT_FILE}")

    # for datastream, data in datastream_sizes.items():
    #     log_message(f"\nDatastream: {datastream}")
    #     log_message(f" Total Size: {data['total_size']} bytes")
    #     for index in data['indices']:
    #         log_message(f" Index: {index}")


if __name__ == "__main__":
    main()
