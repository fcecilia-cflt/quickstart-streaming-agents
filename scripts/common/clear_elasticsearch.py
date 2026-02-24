#!/usr/bin/env python3
"""
Clear Elasticsearch documents index.

Connects to Elasticsearch using REST API and clears all documents from the vector search index.

Usage:
    uv run clear_elasticsearch              # Auto-detect cloud provider and clear index
    uv run clear_elasticsearch aws          # Use AWS credentials
    uv run clear_elasticsearch azure        # Use Azure credentials
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict

import urllib3

from .cloud_detection import auto_detect_cloud_provider, validate_cloud_provider, suggest_cloud_provider
from .terraform import get_project_root


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)


def extract_elasticsearch_credentials(cloud_provider: str, project_root: Path, lab: int = 3) -> Dict[str, str]:
    """
    Extract Elasticsearch credentials from terraform.tfvars.

    Args:
        cloud_provider: Cloud provider (aws or azure)
        project_root: Project root directory
        lab: Lab number (2 or 3)

    Returns:
        Dictionary with Elasticsearch connection details

    Raises:
        Exception if credentials cannot be extracted
    """
    lab_dir = "lab2-vector-search" if lab == 2 else "lab3-agentic-fleet-management"
    tfvars_path = project_root / cloud_provider / lab_dir / "terraform.tfvars"

    if not tfvars_path.exists():
        raise Exception(f"terraform.tfvars not found at {tfvars_path}")

    credentials = {}

    # Lab2 uses unsuffixed variable names, Lab3 uses _lab3 suffix
    endpoint_keys = ['elasticsearch_endpoint', 'elasticsearch_endpoint_lab3']
    api_key_keys = ['elasticsearch_api_key', 'elasticsearch_api_key_lab3']
    index_keys = ['elasticsearch_index', 'elasticsearch_index_lab3']

    with open(tfvars_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or not line:
                continue

            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")

                if key in endpoint_keys:
                    credentials['endpoint'] = value
                elif key in api_key_keys:
                    credentials['api_key'] = value
                elif key in index_keys:
                    credentials['index'] = value
                elif key == 'vector_db':
                    credentials['vector_db'] = value

    # Set defaults if not found
    if 'index' not in credentials:
        credentials['index'] = 'documents_embed'

    # Check if Elasticsearch is configured
    if credentials.get('vector_db') != 'elasticsearch':
        raise Exception("Elasticsearch is not configured as vector_db in terraform.tfvars")

    # Validate required credentials
    required = ['endpoint', 'api_key']
    missing = [key for key in required if key not in credentials or not credentials[key]]
    if missing:
        raise Exception(f"Missing required Elasticsearch credentials: {', '.join(missing)}")

    return credentials


def clear_elasticsearch_index(
    endpoint: str,
    api_key: str,
    index: str = "documents_embed"
) -> int:
    """
    Clear all documents from Elasticsearch index using REST API.

    Args:
        endpoint: Elasticsearch endpoint URL
        api_key: Elasticsearch API key
        index: Index name (default: documents_embed)

    Returns:
        Number of documents deleted

    Raises:
        Exception if connection or delete fails
    """
    # Create HTTP pool manager
    http = urllib3.PoolManager()

    # Set up headers with API key auth
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json"
    }

    # Normalize endpoint (remove trailing slash)
    endpoint = endpoint.rstrip('/')

    # Check if index exists (HEAD request)
    try:
        response = http.request('HEAD', f"{endpoint}/{index}", headers=headers)
        if response.status == 404:
            return 0  # Index doesn't exist
    except Exception as e:
        raise Exception(f"Failed to connect to Elasticsearch: {e}")

    # Count documents
    try:
        response = http.request('GET', f"{endpoint}/{index}/_count", headers=headers)
        if response.status != 200:
            error_msg = response.data.decode('utf-8') if response.data else f"HTTP {response.status}"
            raise Exception(f"Failed to count documents: {error_msg}")
        count_data = json.loads(response.data.decode('utf-8'))
        doc_count = count_data.get('count', 0)
    except json.JSONDecodeError as e:
        raise Exception(f"Failed to parse count response: {e}")

    if doc_count == 0:
        return 0

    # Delete all documents
    try:
        delete_body = json.dumps({"query": {"match_all": {}}}).encode('utf-8')
        response = http.request(
            'POST',
            f"{endpoint}/{index}/_delete_by_query?refresh=true",
            headers=headers,
            body=delete_body
        )

        if response.status not in [200, 201]:
            error_msg = response.data.decode('utf-8') if response.data else f"HTTP {response.status}"
            raise Exception(f"Failed to delete documents: {error_msg}")
    except Exception as e:
        if "Failed to delete" in str(e):
            raise
        raise Exception(f"Failed to delete documents: {e}")

    return doc_count


def main():
    """Main entry point for Elasticsearch index clearer."""
    parser = argparse.ArgumentParser(
        description="Clear Elasticsearch documents index",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s aws
  %(prog)s azure --verbose
        """
    )

    parser.add_argument(
        "cloud_provider",
        nargs="?",
        choices=["aws", "azure"],
        help="Cloud provider (aws or azure). If not specified, will auto-detect."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set up logging
    logger = setup_logging(args.verbose)

    # Determine cloud provider
    cloud_provider = args.cloud_provider
    if not cloud_provider:
        cloud_provider = auto_detect_cloud_provider()
        if not cloud_provider:
            suggestion = suggest_cloud_provider()
            if suggestion:
                logger.info(f"Auto-detected cloud provider: {suggestion}")
                cloud_provider = suggestion
            else:
                logger.error("Could not auto-detect cloud provider. Please specify 'aws' or 'azure'.")
                return 1

    # Validate cloud provider
    if not validate_cloud_provider(cloud_provider):
        logger.error(f"Invalid cloud provider: {cloud_provider}")
        return 1

    # Get project root
    try:
        project_root = get_project_root()
    except Exception as e:
        logger.error(f"Could not find project root: {e}")
        return 1

    # Extract Elasticsearch credentials
    try:
        credentials = extract_elasticsearch_credentials(cloud_provider, project_root)
        logger.debug(f"Extracted credentials for index '{credentials['index']}'")
    except Exception as e:
        logger.error(f"Failed to extract Elasticsearch credentials: {e}")
        return 1

    # Clear index
    try:
        logger.info(f"Connecting to Elasticsearch (index: {credentials['index']})...")

        deleted_count = clear_elasticsearch_index(
            endpoint=credentials['endpoint'],
            api_key=credentials['api_key'],
            index=credentials['index']
        )

        print(f"\n{'=' * 60}")
        print("ELASTICSEARCH INDEX CLEARED")
        print(f"{'=' * 60}")
        print(f"Index:             {credentials['index']}")
        print(f"Documents deleted: {deleted_count}")
        print(f"{'=' * 60}\n")

        return 0

    except Exception as e:
        logger.error(f"Failed to clear Elasticsearch index: {e}")
        print("\nPlease clear your Elasticsearch index manually.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
