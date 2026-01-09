#!/usr/bin/env python3
"""
Simple deployment script for Confluent streaming agents quickstart.
Uses credentials from credentials.env or credentials.json and deploys via Terraform.
"""

import argparse
import os
import subprocess
import sys

from dotenv import dotenv_values, set_key

from scripts.common.credentials import (
    load_or_create_credentials_file,
    load_credentials_json,
    generate_confluent_api_keys
)
from scripts.common.elasticsearch_connector import ensure_elasticsearch_connector
from scripts.common.login_checks import check_confluent_login, check_cloud_cli_login
# Note: decode_elasticsearch_api_key removed - using HTTP Sink V2 with API key header instead
from scripts.common.terraform import get_project_root
from scripts.common.terraform_runner import run_terraform
from scripts.common.tfvars import write_tfvars_for_deployment
from scripts.common.ui import prompt_choice, prompt_with_default

# Valid cloud regions (MongoDB M0 free tier compatible)
AWS_REGIONS = [
    "us-east-1", "us-west-2", "sa-east-1",
    "ap-southeast-1", "ap-southeast-2", "ap-south-1",
    "ap-east-1", "ap-northeast-1", "ap-northeast-2"
]

AZURE_REGIONS = [
    "eastus2", "westus", "canadacentral",
    "northeurope", "westeurope", "eastasia", "centralindia"
]


def get_terraform_outputs(env_path):
    """Get Terraform outputs as a dictionary."""
    import json
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=env_path,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Warning: Failed to get Terraform outputs from {env_path}")
        return {}

    try:
        outputs = json.loads(result.stdout)
        # Extract values from Terraform output format {"key": {"value": "..."}}
        return {k: v.get("value") for k, v in outputs.items()}
    except json.JSONDecodeError:
        return {}


def deploy_elasticsearch_connector(root, cloud, creds):
    """
    Deploy Elasticsearch HTTP Sink V2 connector via CLI.

    This is called after Terraform deploys lab2-vector-search when Elasticsearch
    is selected. The connector requires CLI deployment because API key authentication
    is only supported via Confluent CLI, not Terraform API.
    """
    # Get core Terraform outputs (contains environment ID, cluster ID, Kafka API keys)
    core_path = root / cloud / "core"
    core_outputs = get_terraform_outputs(core_path)

    if not core_outputs:
        print("Error: Could not get Terraform outputs from core module")
        return False

    # Get required values
    environment_id = core_outputs.get("confluent_environment_id")
    cluster_id = core_outputs.get("confluent_kafka_cluster_id")
    kafka_api_key = core_outputs.get("app_manager_kafka_api_key")
    kafka_api_secret = core_outputs.get("app_manager_kafka_api_secret")

    if not all([environment_id, cluster_id, kafka_api_key, kafka_api_secret]):
        print("Error: Missing required Terraform outputs from core module")
        print(f"  environment_id: {environment_id}")
        print(f"  cluster_id: {cluster_id}")
        print(f"  kafka_api_key: {'set' if kafka_api_key else 'missing'}")
        print(f"  kafka_api_secret: {'set' if kafka_api_secret else 'missing'}")
        return False

    # Get Elasticsearch credentials (from creds dict or environment)
    # Handle both TF_VAR_ prefixed and non-prefixed keys
    elasticsearch_endpoint = creds.get("elasticsearch_endpoint") or creds.get("TF_VAR_elasticsearch_endpoint") or os.environ.get("TF_VAR_elasticsearch_endpoint")
    elasticsearch_api_key = creds.get("elasticsearch_api_key") or creds.get("TF_VAR_elasticsearch_api_key") or os.environ.get("TF_VAR_elasticsearch_api_key")
    elasticsearch_index = creds.get("elasticsearch_index") or creds.get("TF_VAR_elasticsearch_index") or os.environ.get("TF_VAR_elasticsearch_index") or "documents-vector"

    if not elasticsearch_endpoint or not elasticsearch_api_key:
        print("Error: Missing Elasticsearch credentials")
        print(f"  elasticsearch_endpoint: {elasticsearch_endpoint}")
        print(f"  elasticsearch_api_key: {'set' if elasticsearch_api_key else 'missing'}")
        return False

    # Create the connector via CLI
    return ensure_elasticsearch_connector(
        environment_id=environment_id,
        cluster_id=cluster_id,
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        elasticsearch_endpoint=elasticsearch_endpoint,
        elasticsearch_api_key=elasticsearch_api_key,
        elasticsearch_index=elasticsearch_index
    )


def main():
    """Main entry point for deploy."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Simple deployment tool for Confluent streaming agents")
    parser.add_argument("--testing", action="store_true",
                       help="Non-interactive mode using credentials.json (for automated testing)")
    parser.add_argument("--workshop", action="store_true",
                       help="Workshop mode using pre-provided cloud credentials (no cloud CLI required)")
    args = parser.parse_args()

    print("=== Simple Deployment Tool ===\n")
    if args.testing:
        print("Running in TESTING mode (non-interactive)\n")
    if args.workshop:
        print("Running in WORKSHOP mode (pre-provided cloud credentials)\n")

    root = get_project_root()
    print(f"Project root: {root}")

    # TESTING MODE: Load credentials from JSON and skip all prompts
    if args.testing:
        creds = load_credentials_json(root)

        # Extract values from JSON
        cloud = creds["cloud"]
        region = creds["region"]
        workshop_mode = creds.get("workshop", False)
        envs_to_deploy = ["core", "lab1-tool-calling", "lab2-vector-search", "lab3-agentic-fleet-management"]

        # Build environment variables for Terraform
        env_vars = {
            "TF_VAR_confluent_cloud_api_key": creds["confluent_cloud_api_key"],
            "TF_VAR_confluent_cloud_api_secret": creds["confluent_cloud_api_secret"],
            "TF_VAR_cloud_region": region,
            "TF_VAR_workshop_mode": "true" if workshop_mode else "false",
        }

        # Optional fields
        if "owner_email" in creds and creds["owner_email"]:
            env_vars["TF_VAR_owner_email"] = creds["owner_email"]
        if "zapier_sse_endpoint" in creds and creds["zapier_sse_endpoint"]:
            env_vars["TF_VAR_zapier_sse_endpoint"] = creds["zapier_sse_endpoint"]
        if "mongodb_connection_string" in creds and creds["mongodb_connection_string"]:
            env_vars["TF_VAR_mongodb_connection_string"] = creds["mongodb_connection_string"]
        if "mongodb_username" in creds and creds["mongodb_username"]:
            env_vars["TF_VAR_mongodb_username"] = creds["mongodb_username"]
        if "mongodb_password" in creds and creds["mongodb_password"]:
            env_vars["TF_VAR_mongodb_password"] = creds["mongodb_password"]

        # Azure subscription ID (use placeholder in workshop mode)
        if cloud == "azure":
            if workshop_mode:
                env_vars["TF_VAR_azure_subscription_id"] = "00000000-0000-0000-0000-000000000000"
            elif "azure_subscription_id" in creds:
                env_vars["TF_VAR_azure_subscription_id"] = creds["azure_subscription_id"]

        # Workshop mode credentials
        if workshop_mode and cloud == "aws":
            if "aws_bedrock_access_key" in creds and creds["aws_bedrock_access_key"]:
                env_vars["TF_VAR_aws_bedrock_access_key"] = creds["aws_bedrock_access_key"]
            if "aws_bedrock_secret_key" in creds and creds["aws_bedrock_secret_key"]:
                env_vars["TF_VAR_aws_bedrock_secret_key"] = creds["aws_bedrock_secret_key"]
        if workshop_mode and cloud == "azure":
            if "azure_openai_endpoint" in creds and creds["azure_openai_endpoint"]:
                env_vars["TF_VAR_azure_openai_endpoint"] = creds["azure_openai_endpoint"]
            if "azure_openai_api_key" in creds and creds["azure_openai_api_key"]:
                env_vars["TF_VAR_azure_openai_api_key"] = creds["azure_openai_api_key"]

        print(f"✓ Credentials loaded from credentials.json")
        print(f"  Cloud: {cloud}")
        print(f"  Region: {region}")
        print(f"  Deploying: {', '.join(envs_to_deploy)}")
        print()

        # Write terraform.tfvars files
        write_tfvars_for_deployment(root, cloud, region, creds, envs_to_deploy)

        # Load into environment
        for key, value in env_vars.items():
            os.environ[key] = value

    # INTERACTIVE MODE: Original flow
    else:
        # Step 0: Check Confluent CLI login
        if not check_confluent_login():
            print("\nError: Not logged into Confluent Cloud.")
            print("Please run: confluent login")
            sys.exit(1)
        print("✓ Confluent CLI logged in")

        # Step 1: Select cloud provider
        cloud = prompt_choice("Select cloud provider:", ["aws", "azure"])

        # Check if Azure workshop mode (not supported yet)
        if args.workshop and cloud == "azure":
            print("\n" + "="*70)
            print("  Workshop mode for Azure is in development but not yet supported.")
            print("  Please either:")
            print("    - Deploy normally without the `--workshop` flag")
            print("    - Try again at a later date")
            print("="*70 + "\n")
            sys.exit(0)

        # Step 1.5: Check cloud CLI login (skip in workshop mode)
        if args.workshop:
            print(f"✓ Workshop mode: Using pre-provided {cloud.upper()} credentials (no CLI login required)")
        else:
            if not check_cloud_cli_login(cloud):
                print(f"\n{'='*70}")
                print(f"  WARNING: You are NOT logged into the {cloud.upper()} CLI!")
                print(f"{'='*70}")
                print(f"  Deployment may fail without proper {cloud.upper()} authentication.")
                print(f"  To login, run: {'aws configure' if cloud == 'aws' else 'az login'}")
                print(f"{'='*70}\n")

                # Ask user to confirm continuation
                while True:
                    response = input("Do you want to continue without CLI authentication? (y/n): ").strip().lower()
                    if response in ['y', 'yes']:
                        print("Continuing deployment without CLI authentication...\n")
                        break
                    elif response in ['n', 'no']:
                        print("Deployment cancelled. Please login and try again.")
                        sys.exit(0)
                    else:
                        print("Invalid input. Please enter 'y' or 'n'.")
            else:
                print(f"✓ {cloud.upper()} CLI logged in")

        # Step 2: Select cloud region (auto-select in workshop mode)
        if args.workshop:
            region = "us-east-1" if cloud == "aws" else "eastus2"
            print(f"✓ Workshop mode: Auto-selected region: {region}")
        else:
            regions = AWS_REGIONS if cloud == "aws" else AZURE_REGIONS
            region = prompt_choice("Select cloud region:", regions)

        # Load credentials file
        creds_file, creds = load_or_create_credentials_file(root)

        # Step 3: Generate Confluent API keys (optional)
        generate = input("\nGenerate new Confluent Cloud API keys? (y/n): ").strip().lower()
        if generate == "y":
            api_key, api_secret = generate_confluent_api_keys()
            if api_key and api_secret:
                set_key(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
                set_key(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)
                creds["TF_VAR_confluent_cloud_api_key"] = api_key
                creds["TF_VAR_confluent_cloud_api_secret"] = api_secret

        # Step 4: Select what to deploy
        envs_to_deploy = []
        deploy_options = [
            "Lab 1: MCP Tool Calling",
            "Lab 2: Vector Search / RAG",
            "Lab 3: Agentic Fleet Management",
            "All Labs (Labs 1, 2, and 3)"
        ]
        env_choice = prompt_choice("What would you like to deploy?", deploy_options)

        # Map user-friendly choice to deployment targets (core auto-included for labs)
        if env_choice == "Lab 1: MCP Tool Calling":
            envs_to_deploy = ["core", "lab1-tool-calling"]
        elif env_choice == "Lab 2: Vector Search / RAG":
            envs_to_deploy = ["core", "lab2-vector-search"]
        elif env_choice == "Lab 3: Agentic Fleet Management":
            # In workshop mode, Lab3 is standalone (has its own MongoDB connection)
            if args.workshop:
                envs_to_deploy = ["core", "lab3-agentic-fleet-management"]
            else:
                envs_to_deploy = ["core", "lab2-vector-search", "lab3-agentic-fleet-management"]
        elif env_choice == "All Labs (Labs 1, 2, and 3)":
            envs_to_deploy = ["core", "lab1-tool-calling", "lab2-vector-search", "lab3-agentic-fleet-management"]

        # Step 5: Prompt for required credentials
        print("\n--- Credential Configuration ---")

        # Confluent credentials (always required)
        api_key = prompt_with_default("Confluent Cloud API Key", creds.get("TF_VAR_confluent_cloud_api_key", ""))
        api_secret = prompt_with_default("Confluent Cloud API Secret", creds.get("TF_VAR_confluent_cloud_api_secret", ""))
        set_key(creds_file, "TF_VAR_confluent_cloud_api_key", api_key)
        set_key(creds_file, "TF_VAR_confluent_cloud_api_secret", api_secret)

        # Owner email (optional, for resource tagging)
        owner_email = prompt_with_default("Owner Email (for AWS/Azure resource tagging)", creds.get("TF_VAR_owner_email", ""))
        if owner_email:
            set_key(creds_file, "TF_VAR_owner_email", owner_email)

        # Azure subscription ID
        if cloud == "azure" and "core" in envs_to_deploy:
            if args.workshop:
                # Workshop mode: use placeholder since no Azure resources are created
                azure_sub = "00000000-0000-0000-0000-000000000000"
                set_key(creds_file, "TF_VAR_azure_subscription_id", azure_sub)
            else:
                # Production mode: prompt for real subscription ID
                azure_sub = prompt_with_default("Azure Subscription ID", creds.get("TF_VAR_azure_subscription_id", ""))
                set_key(creds_file, "TF_VAR_azure_subscription_id", azure_sub)

        # Workshop mode: AWS Bedrock credentials (pre-provided)
        if args.workshop and cloud == "aws":
            aws_bedrock_key = prompt_with_default("AWS Bedrock Access Key (workshop)", creds.get("TF_VAR_aws_bedrock_access_key", ""))
            aws_bedrock_secret = prompt_with_default("AWS Bedrock Secret Key (workshop)", creds.get("TF_VAR_aws_bedrock_secret_key", ""))
            set_key(creds_file, "TF_VAR_aws_bedrock_access_key", aws_bedrock_key)
            set_key(creds_file, "TF_VAR_aws_bedrock_secret_key", aws_bedrock_secret)

        # Workshop mode: Azure OpenAI credentials (pre-provided)
        if args.workshop and cloud == "azure":
            azure_openai_endpoint = prompt_with_default("Azure OpenAI Endpoint (workshop)", creds.get("TF_VAR_azure_openai_endpoint", ""))
            azure_openai_key = prompt_with_default("Azure OpenAI API Key (workshop)", creds.get("TF_VAR_azure_openai_api_key", ""))
            set_key(creds_file, "TF_VAR_azure_openai_endpoint", azure_openai_endpoint)
            set_key(creds_file, "TF_VAR_azure_openai_api_key", azure_openai_key)

        # Workshop mode: Vector database selection for Lab3 (no credential prompts, uses defaults)
        if args.workshop and "lab3-agentic-fleet-management" in envs_to_deploy:
            print("\n--- Vector Database Selection (Lab3 Workshop Mode) ---")
            vector_db_options = ["MongoDB (default)", "Elasticsearch"]
            vector_db_choice = prompt_choice("Select vector database for Lab3:", vector_db_options)

            if vector_db_choice == "Elasticsearch":
                set_key(creds_file, "TF_VAR_vector_db", "elasticsearch")
            else:
                set_key(creds_file, "TF_VAR_vector_db", "mongodb")

        # Lab-specific credentials
        if "lab1-tool-calling" in envs_to_deploy or "lab3-agentic-fleet-management" in envs_to_deploy:
            zapier_endpoint = prompt_with_default("Zapier SSE Endpoint (Lab 1 and Lab 3)", creds.get("TF_VAR_zapier_sse_endpoint", ""))
            set_key(creds_file, "TF_VAR_zapier_sse_endpoint", zapier_endpoint)

        # Non-workshop mode: Vector database selection (applies to Lab2 and Lab3)
        vector_db_choice = "mongodb"  # default
        if not args.workshop and ("lab2-vector-search" in envs_to_deploy or "lab3-agentic-fleet-management" in envs_to_deploy):
            print("\n--- Vector Database Selection ---")
            vector_db_options = ["MongoDB (default)", "Elasticsearch"]
            vector_db_choice = prompt_choice("Select vector database:", vector_db_options)
            if vector_db_choice == "Elasticsearch":
                set_key(creds_file, "TF_VAR_vector_db", "elasticsearch")
            else:
                set_key(creds_file, "TF_VAR_vector_db", "mongodb")
                vector_db_choice = "mongodb"

        # MongoDB credentials needed if:
        # - Lab2 or Lab3 is being deployed in non-workshop mode with MongoDB selected
        # In workshop mode, both Lab2 and Lab3 use hardcoded MongoDB credentials
        needs_mongodb = not args.workshop and (
            ("lab2-vector-search" in envs_to_deploy or "lab3-agentic-fleet-management" in envs_to_deploy) and
            vector_db_choice != "Elasticsearch"
        )

        if needs_mongodb:
            mongo_conn = prompt_with_default("MongoDB Connection String (Lab 2 and Lab 3)", creds.get("TF_VAR_mongodb_connection_string", ""))
            mongo_user = prompt_with_default("MongoDB Username (Lab 2 and Lab 3)", creds.get("TF_VAR_mongodb_username", ""))
            mongo_pass = prompt_with_default("MongoDB Password (Lab 2 and Lab 3)", creds.get("TF_VAR_mongodb_password", ""))
            set_key(creds_file, "TF_VAR_mongodb_connection_string", mongo_conn)
            set_key(creds_file, "TF_VAR_mongodb_username", mongo_user)
            set_key(creds_file, "TF_VAR_mongodb_password", mongo_pass)

        # Elasticsearch credentials needed if Lab2 or Lab3 in non-workshop mode with Elasticsearch selected
        needs_elasticsearch = not args.workshop and \
            ("lab2-vector-search" in envs_to_deploy or "lab3-agentic-fleet-management" in envs_to_deploy) and \
            vector_db_choice == "Elasticsearch"

        if needs_elasticsearch:
            es_endpoint = prompt_with_default("Elasticsearch Endpoint URL (Lab 2 and Lab 3)", creds.get("TF_VAR_elasticsearch_endpoint", ""))
            es_api_key = prompt_with_default("Elasticsearch API Key (Lab 2 and Lab 3)", creds.get("TF_VAR_elasticsearch_api_key", ""))
            set_key(creds_file, "TF_VAR_elasticsearch_endpoint", es_endpoint)
            set_key(creds_file, "TF_VAR_elasticsearch_api_key", es_api_key)
            # Note: Using HTTP Sink V2 with API key in Authorization header
            # No need to decode credentials - API key is used directly

        # Set cloud region
        set_key(creds_file, "TF_VAR_cloud_region", region)

        # Set workshop mode flag
        set_key(creds_file, "TF_VAR_workshop_mode", "true" if args.workshop else "false")

        # Step 5.5: Validate configurations (advisory only, never blocks deployment)
        needs_zapier = "lab1-tool-calling" in envs_to_deploy or "lab3-agentic-fleet-management" in envs_to_deploy
        needs_mongodb = not args.workshop and (
            ("lab2-vector-search" in envs_to_deploy or "lab3-agentic-fleet-management" in envs_to_deploy) and
            vector_db_choice != "Elasticsearch"
        )

        if needs_zapier or needs_mongodb:
            print("\n--- Configuration Validation (Advisory Only) ---")

            # Load credentials into environment for validation
            temp_creds = dotenv_values(creds_file)
            for key, value in temp_creds.items():
                if value:
                    os.environ[key] = value

            # Validate Zapier
            if needs_zapier:
                try:
                    result = subprocess.run(
                        ["uv", "run", "validate", "zapier"],
                        cwd=root,
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    if "ALL VALIDATION CHECKS PASSED" in result.stdout:
                        print("✓ Zapier configuration validated")
                    else:
                        print(result.stdout)
                        response = input("\nZapier validation warnings detected. Continue anyway? (y/n): ")
                        if response.lower() != 'y':
                            sys.exit(1)
                except Exception as e:
                    print(f"⚠ Could not validate Zapier configuration: {e}")
                    print("  (This is advisory only - deployment will continue)")

            # Validate MongoDB
            if needs_mongodb:
                try:
                    result = subprocess.run(
                        ["uv", "run", "validate", "mongodb"],
                        cwd=root,
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    if "ALL VALIDATION CHECKS PASSED" in result.stdout:
                        print("✓ MongoDB configuration validated")
                    else:
                        print(result.stdout)
                        response = input("\nMongoDB validation warnings detected. Continue anyway? (y/n): ")
                        if response.lower() != 'y':
                            sys.exit(1)
                except Exception as e:
                    print(f"⚠ Could not validate MongoDB configuration: {e}")
                    print("  (This is advisory only - deployment will continue)")

            print()

        # Step 6: Show all credentials and confirm
        print("\n--- Configuration Summary ---")
        final_creds = dotenv_values(creds_file)
        for key, value in sorted(final_creds.items()):
            if value:
                print(f"{key}: {value}")

        print(f"\nCloud: {cloud}")
        print(f"Region: {region}")
        print(f"Deploying: {', '.join(envs_to_deploy)}")

        confirm = input("\nReady to deploy? (y/n): ").strip().lower()
        if confirm != "y":
            print("Deployment cancelled.")
            sys.exit(0)

        # Step 6.5: Write terraform.tfvars files
        print()
        write_tfvars_for_deployment(root, cloud, region, final_creds, envs_to_deploy)

        # Step 7: Load credentials into environment and deploy
        for key, value in final_creds.items():
            if value:
                os.environ[key] = value

    # Determine which creds dict to use (testing mode vs interactive)
    # In testing mode, 'creds' is already defined; in interactive mode, use 'final_creds'
    deployment_creds = creds if args.testing else final_creds

    # Check if Elasticsearch is selected as vector database
    is_elasticsearch = (
        deployment_creds.get("vector_db") == "elasticsearch" or
        deployment_creds.get("TF_VAR_vector_db") == "elasticsearch"
    )

    print("\n=== Starting Deployment ===")
    for env in envs_to_deploy:
        env_path = root / cloud / env
        if not env_path.exists():
            print(f"Warning: {env_path} does not exist, skipping.")
            continue

        if not run_terraform(env_path):
            print(f"\nDeployment failed at {env}. Stopping.")
            sys.exit(1)

        # TEMPORARY WORKAROUND: Create Elasticsearch connector via CLI
        # HTTP Sink V2 API key auth only works via CLI, not Terraform.
        # TODO: Remove when Elasticsearch Sink V2 supports API key auth.
        # See: scripts/common/elasticsearch_connector.py
        if env == "lab2-vector-search" and is_elasticsearch and not args.workshop:
            print("\n--- Creating Elasticsearch Connector via CLI ---")
            if not deploy_elasticsearch_connector(root, cloud, deployment_creds):
                print("\nWarning: Elasticsearch connector creation failed.")
                print("You can create it manually using: confluent connect cluster create")
                # Don't fail deployment - connector can be created manually

    print("\n✓ All deployments completed successfully!")


if __name__ == "__main__":
        main()
