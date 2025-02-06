import csv
import re
import pandas as pd
import argparse
import os
import hashlib
from elasticsearch import Elasticsearch, helpers


def parse_index_name(index_name):
    
    if "." in index_name and not index_name.startswith(".ds-"):
        parts = index_name.split(".")

        dataset = parts[0] if len(parts) > 0 else None
        namespace = ".".join(parts[1:-1]) if len(parts) > 2 else None
        suffix = parts[-1] if len(parts) > 1 else None

        environment = "default"
        if suffix and re.match(r"\d+\.\d+\.\d+", suffix):
            namespace = ".".join(parts[1:-2]) if len(parts) > 3 else None
            environment = parts[-2] if len(parts) > 2 else "default"

        application = f"{dataset}"
        if namespace:
            application += f".{namespace}"

        return {
            "type": "logs",
            "dataset": dataset,
            "namespace": namespace,
            "environment": environment,
            "application": application,
            "date": None,
            "iteration": None,
        }

    if index_name.startswith(".ds-") or ".ds-" in index_name or re.search(r"\.ds-[\w\.-]", index_name):
        print("special case")

        stripped_name = index_name.lstrip(".ds-")
        parts = stripped_name.split("-")

        dataset = parts[0]
        namespace = "-".join(parts[1:-2]) if len(parts) > 2 else None
        date = parts[-2] if len(parts) > 1 and re.match(r"\d{4}\.\d{2}\.\d{2}", parts[-2]) else None
        iteration = parts[-1] if len(parts) > 1 and re.match(r"\d+", parts[-1]) else None

        if namespace is None and len(parts) > 1:
            namespace = "-".join(parts[1:])

        environment = None
        if namespace and "-" in namespace:
            namespace_split = namespace.rsplit("-", 1)
            namespace = namespace_split[0]
            environment = namespace_split[1]

        if not namespace:
            environment = "default"

        application = f"{dataset}"
        if namespace:
            application += f".{namespace}"


        return {
            "type": "logs",
            "dataset": dataset,
            "namespace": namespace,
            "environment": application,
            "date": date.replace(".", "-") if date else None,
            "iteration": iteration
        }

    primary_pattern = (
        r"\.ds-(?P<type>\w+)-(?P<dataset>\w+)(?:\.(?P<namespace>[\w\.\-]+))?-(?P<created_date>\d{4}\.\d{2}\.\d{2})-(?P<iteration>\d+)"
    )

    match = re.match(primary_pattern, index_name)
    
    if not match:
        
        secondary_pattern = (
            r"\.ds-(?P<type>\w+)-(?P<dataset>\w+)-(?P<namespace>\w+)-(?P<created_date>\d{4}\.\d{2}\.\d{2})-(?P<iteration>\d+)"
        )
        match = re.match(secondary_pattern, index_name)

    if match:
        dataset = match.group("dataset")
        namespace = match.group("namespace")
        environment = None

        if namespace:
            if "-" in namespace:
                namespace_split = namespace.rsplit("-", 1)
                namespace = namespace_split[0]
                environment = namespace_split[1]
  
        else:
            environment = "default"

        application = f"{dataset}"
        if namespace:
            application += f".{namespace}"

        return {
            "type": match.group("type"),
            "dataset": dataset,
            "namespace": namespace,
            "environment": environment,
            "application": application,
            "created_date": match.group("created_date"),
            "iteration": match.group("iteration")
        }
    
    return {
        "type": None,
        "dataset": None,
        "namespace": None,
        "created_date": None,
        "iteration": None,
        "namespace": None,
        "application": None,
    }


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Process CSV file to extract index_name details.")
    parser.add_argument("-f", "--file", required=True,
                        help="Path to input CSV file")
    parser.add_argument("-o", "--output", help="Output to file")
    parser.add_argument("--ingest", action="store_true", help="Enable ingest to Elasticsearch")
    return parser.parse_args()

def generate_document_id(index_name):
    return hashlib.sha256(index_name.encode()).hexdigest()

def ingest_to_elasticsearch(es_host, es_index, es_api_key, data):
    es = Elasticsearch(
        es_host,
        api_key=es_api_key
    )

    actions = [
        {
            "_id": generate_document_id(record["index_name"]),
            "_index": es_index,
            "_source": record
        }
        for record in data
    ]
    helpers.bulk(es, actions)
    print(f"Data ingested into Elasticsearch index: {es_index}")


def main():
    args = parse_arguments()
    input_file = args.file
    output_file = args.output

    es_host = os.getenv("ES_HOST")
    es_index = os.getenv("ES_INDEX")
    es_api_key = os.getenv("ES_API_KEY")

    if args.ingest and not es_api_key:
        print("Error: Elastic API key (ES_API_KEY) i snot set in environment variables")
        exit(1)

    cluster_name = input_file.split('.')[0]

    data = []
    with open(input_file, "r") as file:
        reader = csv.reader(file, delimiter=';')
        header = next(reader)
        index_col = header.index("index")

        first_timestamp_col = header.index("first_timestamp")
        last_timestamp_col = header.index("last_timestamp")
        daily_ingest_mb_col = header.index("daily_ingest_mb")

        for row in reader:
            index_name = row[index_col]
            parsed_data = parse_index_name(index_name)

            daily_ingest_mb = float(row[daily_ingest_mb_col].replace(",", "."))
            daily_ingest_bytes = int(daily_ingest_mb * 1024 * 1024)

            record = {
                "index_name": index_name,
                "cluster": cluster_name,
                "first_timestamp": row[first_timestamp_col],
                "last_timestamp": row[last_timestamp_col],
                "daily_ingest_bytes": daily_ingest_bytes,
                **parsed_data
            }

            data.append(record)

    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)

    print(f"Processed data saved to {output_file}")

    if args.ingest:
        ingest_to_elasticsearch(es_host, es_index, es_api_key, data)

if __name__ == "__main__":
    main()
