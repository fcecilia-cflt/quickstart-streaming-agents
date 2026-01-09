#!/usr/bin/env python3
"""Elasticsearch authentication utilities for Confluent Sink Connector."""

import base64
import json
import secrets
import string
import urllib3

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def generate_password(length: int = 32) -> str:
    """Generate a secure random password."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def decode_elasticsearch_api_key(api_key: str) -> tuple[str, str]:
    """
    Decode Elasticsearch API key to extract id and secret.

    Elasticsearch API keys are base64-encoded 'id:secret' pairs.
    This works for both standard Elasticsearch and Elasticsearch Serverless.

    Args:
        api_key: Base64-encoded Elasticsearch API key

    Returns:
        Tuple of (api_key_id, api_key_secret) which can be used as (username, password)

    Raises:
        ValueError: If API key format is invalid
    """
    try:
        decoded = base64.b64decode(api_key).decode('utf-8')
    except Exception as e:
        raise ValueError(f"Failed to decode API key (not valid base64): {e}")

    if ':' not in decoded:
        raise ValueError("Invalid API key format - expected base64-encoded 'id:secret'")

    parts = decoded.split(':', 1)
    return parts[0], parts[1]  # (id, secret)


def create_elasticsearch_user(
    endpoint: str,
    api_key: str,
    username: str = "confluent_connector"
) -> tuple[str, str]:
    """
    Create a user in Elasticsearch using API key authentication.

    Args:
        endpoint: Elasticsearch endpoint URL (e.g., https://my-cluster.es.us-east-1.aws.found.io)
        api_key: Elasticsearch API key for authentication
        username: Username to create (default: confluent_connector)

    Returns:
        Tuple of (username, password)

    Raises:
        Exception: If user creation fails
    """
    http = urllib3.PoolManager(cert_reqs='CERT_NONE')
    password = generate_password()

    # Normalize endpoint URL
    endpoint = endpoint.rstrip('/')

    url = f"{endpoint}/_security/user/{username}"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json"
    }
    body = json.dumps({
        "password": password,
        "roles": ["superuser"],
        "full_name": "Confluent Sink Connector",
        "metadata": {
            "created_by": "quickstart-streaming-agents"
        }
    })

    response = http.request("POST", url, body=body, headers=headers)

    if response.status in [200, 201]:
        return username, password
    else:
        error_msg = response.data.decode() if response.data else "Unknown error"
        raise Exception(f"Failed to create Elasticsearch user: {response.status} - {error_msg}")


def delete_elasticsearch_user(
    endpoint: str,
    api_key: str,
    username: str = "confluent_connector"
) -> bool:
    """
    Delete a user from Elasticsearch using API key authentication.

    Args:
        endpoint: Elasticsearch endpoint URL
        api_key: Elasticsearch API key for authentication
        username: Username to delete (default: confluent_connector)

    Returns:
        True if deleted successfully, False otherwise
    """
    http = urllib3.PoolManager(cert_reqs='CERT_NONE')

    # Normalize endpoint URL
    endpoint = endpoint.rstrip('/')

    url = f"{endpoint}/_security/user/{username}"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json"
    }

    response = http.request("DELETE", url, headers=headers)

    return response.status == 200


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python setup_elasticsearch_user.py <endpoint> <api_key> [username]")
        print("Example: python setup_elasticsearch_user.py https://my-cluster.es.aws.found.io my-api-key")
        sys.exit(1)

    es_endpoint = sys.argv[1]
    es_api_key = sys.argv[2]
    es_username = sys.argv[3] if len(sys.argv) > 3 else "confluent_connector"

    try:
        username, password = create_elasticsearch_user(es_endpoint, es_api_key, es_username)
        print(f"Created Elasticsearch user: {username}")
        print(f"Password: {password}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
