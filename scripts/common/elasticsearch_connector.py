"""
TEMPORARY WORKAROUND: Elasticsearch HTTP Sink V2 Connector via CLI

This module exists because of two limitations:
1. Elasticsearch Sink Connector only supports username/password (not API keys)
2. HTTP Sink V2 supports API key auth, but ONLY via Confluent CLI (not Terraform/API)

Elasticsearch Serverless requires API key authentication, so we must use HTTP Sink V2
deployed via CLI as a workaround.

TODO: Remove this workaround when Elasticsearch Sink V2 supports API key authentication.
      At that point, switch back to Terraform-managed confluent_connector resource.

"""

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Tuple


CONNECTOR_NAME = "elasticsearch-http-sink"


def run_cli_command(args: list[str], capture_output: bool = True) -> Tuple[int, str, str]:
    """
    Run a Confluent CLI command.

    Args:
        args: Command arguments (without 'confluent' prefix)
        capture_output: Whether to capture stdout/stderr

    Returns:
        Tuple of (return_code, stdout, stderr)
    """
    cmd = ["confluent"] + args
    result = subprocess.run(
        cmd,
        capture_output=capture_output,
        text=True
    )
    return result.returncode, result.stdout, result.stderr


def get_existing_connector(
    environment_id: str,
    cluster_id: str
) -> Optional[dict]:
    """
    Check if the Elasticsearch connector already exists.

    Args:
        environment_id: Confluent Cloud environment ID
        cluster_id: Kafka cluster ID

    Returns:
        Connector info dict if exists, None otherwise
    """
    returncode, stdout, stderr = run_cli_command([
        "connect", "cluster", "list",
        "--cluster", cluster_id,
        "--environment", environment_id,
        "--output", "json"
    ])

    if returncode != 0:
        print(f"Warning: Failed to list connectors: {stderr}")
        return None

    try:
        connectors = json.loads(stdout) if stdout.strip() else []
        for connector in connectors:
            if connector.get("name") == CONNECTOR_NAME:
                return connector
    except json.JSONDecodeError:
        pass

    return None


def delete_connector(
    connector_id: str,
    environment_id: str,
    cluster_id: str
) -> bool:
    """
    Delete an existing connector.

    Args:
        connector_id: Connector ID to delete
        environment_id: Confluent Cloud environment ID
        cluster_id: Kafka cluster ID

    Returns:
        True if successful, False otherwise
    """
    print(f"  Deleting existing connector {connector_id}...")
    returncode, stdout, stderr = run_cli_command([
        "connect", "cluster", "delete", connector_id,
        "--cluster", cluster_id,
        "--environment", environment_id,
        "--force"
    ])

    if returncode != 0:
        print(f"  Error deleting connector: {stderr}")
        return False

    print(f"  Deleted connector {connector_id}")
    return True


def create_connector(
    environment_id: str,
    cluster_id: str,
    kafka_api_key: str,
    kafka_api_secret: str,
    elasticsearch_endpoint: str,
    elasticsearch_api_key: str,
    elasticsearch_index: str = "documents-vector"
) -> Tuple[bool, Optional[str]]:
    """
    Create the Elasticsearch HTTP Sink V2 connector via CLI.

    Args:
        environment_id: Confluent Cloud environment ID
        cluster_id: Kafka cluster ID
        kafka_api_key: Kafka API key for the connector
        kafka_api_secret: Kafka API secret for the connector
        elasticsearch_endpoint: Elasticsearch endpoint URL
        elasticsearch_api_key: Elasticsearch API key
        elasticsearch_index: Elasticsearch index name

    Returns:
        Tuple of (success, connector_id)
    """
    # Build connector configuration
    config = {
        "name": CONNECTOR_NAME,
        "connector.class": "HttpSinkV2",
        "kafka.api.key": kafka_api_key,
        "kafka.api.secret": kafka_api_secret,
        "topics": "documents_embed",
        "input.data.format": "AVRO",
        "http.api.base.url": f"{elasticsearch_endpoint}/{elasticsearch_index}/_doc",
        "api1.topics": "documents_embed",
        "api1.request.method": "POST",
        "api1.http.request.headers": f"Content-Type:application/json||Authorization:ApiKey {elasticsearch_api_key}",
        "tasks.max": "1"
    }

    # Write config to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(config, f, indent=2)
        config_file = f.name

    try:
        print(f"  Creating Elasticsearch HTTP Sink V2 connector...")
        returncode, stdout, stderr = run_cli_command([
            "connect", "cluster", "create",
            "--cluster", cluster_id,
            "--environment", environment_id,
            "--config-file", config_file
        ])

        if returncode != 0:
            print(f"  Error creating connector: {stderr}")
            return False, None

        # Parse connector ID from output
        # Output format: "| ID   | lcc-xxxxx |"
        connector_id = None
        for line in stdout.split('\n'):
            if '| ID' in line and '|' in line:
                parts = line.split('|')
                if len(parts) >= 3:
                    connector_id = parts[2].strip()
                    break

        print(f"  Created connector: {connector_id}")
        return True, connector_id

    finally:
        # Clean up temp file
        Path(config_file).unlink(missing_ok=True)


def delete_elasticsearch_connector_by_name(
    environment_id: str,
    cluster_id: str
) -> bool:
    """
    Delete the Elasticsearch connector if it exists.

    This is used during `uv run destroy` to clean up the CLI-created connector
    before Terraform destroys other resources.

    Args:
        environment_id: Confluent Cloud environment ID
        cluster_id: Kafka cluster ID

    Returns:
        True if deleted or didn't exist, False on error
    """
    print(f"  Checking for Elasticsearch connector to delete...")
    existing = get_existing_connector(environment_id, cluster_id)

    if existing:
        connector_id = existing.get("id")
        print(f"  Found connector: {connector_id}")
        return delete_connector(connector_id, environment_id, cluster_id)
    else:
        print(f"  No Elasticsearch connector found (nothing to delete)")
        return True


def ensure_elasticsearch_connector(
    environment_id: str,
    cluster_id: str,
    kafka_api_key: str,
    kafka_api_secret: str,
    elasticsearch_endpoint: str,
    elasticsearch_api_key: str,
    elasticsearch_index: str = "documents-vector"
) -> bool:
    """
    Ensure the Elasticsearch connector exists and is running.

    This function is idempotent:
    - If connector exists and is RUNNING: skip creation
    - If connector exists and is FAILED: delete and recreate
    - If connector doesn't exist: create it

    Args:
        environment_id: Confluent Cloud environment ID
        cluster_id: Kafka cluster ID
        kafka_api_key: Kafka API key for the connector
        kafka_api_secret: Kafka API secret for the connector
        elasticsearch_endpoint: Elasticsearch endpoint URL
        elasticsearch_api_key: Elasticsearch API key
        elasticsearch_index: Elasticsearch index name

    Returns:
        True if connector is running, False otherwise
    """
    print("\n--- Elasticsearch Connector Setup ---")
    print(f"  Environment: {environment_id}")
    print(f"  Cluster: {cluster_id}")
    print(f"  Elasticsearch: {elasticsearch_endpoint}/{elasticsearch_index}")

    # Check for existing connector
    existing = get_existing_connector(environment_id, cluster_id)

    if existing:
        status = existing.get("status", "UNKNOWN")
        connector_id = existing.get("id", "unknown")
        print(f"  Found existing connector: {connector_id} (Status: {status})")

        if status == "RUNNING":
            print("  Connector is already running. Skipping creation.")
            return True
        elif status in ["FAILED", "DEGRADED"]:
            print(f"  Connector is {status}. Deleting and recreating...")
            if not delete_connector(connector_id, environment_id, cluster_id):
                return False
        else:
            print(f"  Connector status is {status}. Attempting to recreate...")
            delete_connector(connector_id, environment_id, cluster_id)

    # Create new connector
    success, connector_id = create_connector(
        environment_id=environment_id,
        cluster_id=cluster_id,
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        elasticsearch_endpoint=elasticsearch_endpoint,
        elasticsearch_api_key=elasticsearch_api_key,
        elasticsearch_index=elasticsearch_index
    )

    if success:
        print(f"  Elasticsearch connector created successfully!")
        return True
    else:
        print(f"  Failed to create Elasticsearch connector.")
        return False
