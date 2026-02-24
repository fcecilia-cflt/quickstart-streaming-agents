variable "cloud_region" {
  description = "Azure region for deployment"
  type        = string
}

variable "azure_subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "workshop_mode" {
  description = "Enable workshop mode (uses pre-provided MongoDB credentials for Lab3 vector search)"
  type        = bool
  default     = false
}

variable "mongodb_connection_string_lab3" {
  description = "MongoDB connection string for Lab3 vector search"
  type        = string
  sensitive   = true
  default     = "mongodb+srv://cluster0.iir6woe.mongodb.net/"
}

variable "mongodb_username_lab3" {
  description = "MongoDB username for Lab3 vector search"
  type        = string
  sensitive   = true
  default     = "public_readonly_user"
}

variable "mongodb_password_lab3" {
  description = "MongoDB password for Lab3 vector search"
  type        = string
  sensitive   = true
  default     = "pE7xOkiKth2QqTKL"
}

variable "zapier_sse_endpoint" {
  description = "Zapier SSE endpoint for MCP connection"
  type        = string
  sensitive   = true
}

# Vector database selection for Lab3
variable "vector_db" {
  description = "Vector database for LAB3 (mongodb or elasticsearch)"
  type        = string
  default     = "mongodb"
  validation {
    condition     = contains(["mongodb", "elasticsearch"], var.vector_db)
    error_message = "vector_db must be 'mongodb' or 'elasticsearch'"
  }
}

# Elasticsearch variables (used when vector_db = "elasticsearch")
variable "elasticsearch_endpoint_lab3" {
  description = "Elasticsearch endpoint URL (e.g., https://my-cluster.es.eastus.azure.elastic-cloud.com:443)"
  type        = string
  sensitive   = true
  default     = "https://workshop-cluster.es.eastus.azure.elastic-cloud.com:443"
}

variable "elasticsearch_api_key_lab3" {
  description = "Elasticsearch API key for authentication"
  type        = string
  sensitive   = true
  default     = "PLACEHOLDER_API_KEY"
}

variable "elasticsearch_index_lab3" {
  description = "Elasticsearch index name for vector search"
  type        = string
  default     = "documents_embed"
}
