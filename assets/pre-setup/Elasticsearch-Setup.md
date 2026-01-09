# Elasticsearch Setup for Lab 3

This guide covers setting up Elasticsearch as an alternative vector database for Lab 3.

## Prerequisites

- Elastic Cloud account (14-day free trial available)
- Confluent Cloud account with Flink enabled

## Step 1: Create Elastic Cloud Deployment

1. Go to [Elastic Cloud](https://cloud.elastic.co/) and sign up or log in
2. Click **Create deployment**
3. Choose your cloud provider and region:
   - **AWS:** Select `us-east-1` (default region for this lab)
   - **Azure:** Select `East US 2` (default region for this lab)
4. Select deployment size (the smallest tier works for this lab)
5. Click **Create deployment**
6. Save the deployment credentials shown after creation

## Step 2: Generate API Key

1. From Elastic Home, go to **API keys**
2. Click **Create API key**
3. Name your API key (e.g., `confluent-flink-lab3`)
4**Important**: Copy and save the API key immediately - it won't be shown again

The API key format is typically: `base64_encoded_id:api_key`

## Step 3: Get Elasticsearch Endpoint

1. In Elastic Cloud, go to your Home
2. Click **Copy endpoint** next to the Elasticsearch endpoint
3. The endpoint looks like: `https://my-deployment.es.us-east-1.aws.elastic-cloud.com:443`

## Step 4: Create Vector Search Index

Before deploying, create the index with dense_vector

```bash
curl -X PUT "https://YOUR_ENDPOINT/documents-vector" \
  -H "Authorization: ApiKey YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": {
      "properties": {
        "document_id": { "type": "keyword" },
        "chunk": { "type": "text" },
        "embedding": {
          "type": "dense_vector",
          "dims": 1536,
          "index": true,
          "similarity": "cosine"
        }
      }
    }
  }'
```

## Step 5: Verify Index Creation

```bash
curl -X GET "https://YOUR_ENDPOINT/documents-vector/_mapping" \
  -H "Authorization: ApiKey YOUR_API_KEY"
```

You should see the mapping with `dense_vector` type for the embedding field.

## Step 6: Deploy Lab 3 with Elasticsearch

When running the deployment:

```bash
uv run deploy
```

1. Select **Lab 3: Agentic Fleet Management**
2. When prompted for vector database, select **Elasticsearch**
3. In non-workshop mode, enter your Elasticsearch endpoint URL and API key
4. In workshop mode, credentials are pre-configured

## Configuration Summary

| Setting | Value |
|---------|-------|
| Endpoint | `https://your-cluster.es.region.cloud-provider.elastic-cloud.com:443` |
| API Key | Your generated API key |
| Index Name | `documents-vector` (default) |
| Vector Dimensions | 1536 (OpenAI embeddings) |
| Similarity | cosine |

## Troubleshooting

### Connection Failed
- Verify your endpoint URL includes the port (`:443`)
- Check that your API key has the correct permissions
- Ensure your Elastic Cloud deployment is running

### Vector Search Not Working
- Check that `embedding` field has `"type": "dense_vector"` with `"index": true`
- Confirm vector dimensions match (1536 for OpenAI embeddings)
- Verify `similarity` is set to `cosine`

### Permission Denied
- Regenerate your API key with appropriate index permissions
- Ensure the API key has at least `read` and `write` access to the index

## Cost Considerations

Unlike MongoDB Atlas (which has a free M0 tier), Elastic Cloud:
- Offers a **14-day free trial**
- Minimum cost after trial: ~$25-30/month

For cost-conscious users, MongoDB remains the recommended option for this lab.

## References

- [Elastic Cloud](https://cloud.elastic.co/)
- [Elasticsearch k-NN Search](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html)
- [Confluent Flink Elasticsearch Connector](https://docs.confluent.io/cloud/current/ai/external-tables/vector-search.html)
