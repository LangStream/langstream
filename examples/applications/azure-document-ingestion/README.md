
Download the secureBundle from Astra and save to azure-document-ingestion/secure-bundle.zip

Add the path to ../secrets/secrets-azure-document-ingestion.yaml in the secureBundle property

Create a secrets file like this:

secrets:
  - id: open-ai
    data:
      access-key: "example-key"
      url: "https://example-domain.openai.azure.com"
      provider: "azure"
      embeddings-model: "text-embedding-ada-002"
      embeddings-deployment: "example-deployment"
      version: "2023-03-15-preview"
      llm: "gpt-4"
  - id: astra
    data:
      token: "AstraCS:example-token"
      database: "example-database"
      secureBundle: "/path/to/secure-connect-vector-demo.zip"
      environment: "prod"
      keyspace: "example_keyspace"
      table: "example_table"
  - id: azure
    data:
      storage-access-key: "example-azure-access-key"
      storage-account-name: "example-azure-storage-account-name"
      container: "example-azure-storage-container-name"

Ensure you've created a database in Astra, and use that database's name instead of "example-database" above.
Download the secure bundle and update the path in the secrets YAML to point to the directory where it will reside.
The Astra keyspace and table will be generated automatically when the pipeline is run.
Populate the appropriate values in the secrets yaml for your Azure storage access key, storage account name, and container.

To run the flow, run these commands:
cd examples/applications/azure-document-ingestion
langstream docker run test -app . -s ../../secrets/secrets-azure-document-ingestion.yaml --langstream-runtime-docker-image public.ecr.aws/y3i6u2n7/datastax-public/langstream-runtime-tester --langstream-runtime-version latest

Ensure that any sensitive files are added to .gitignore

Any PDF files that are added to the Azure blob storage container will be automatically ingested into the flow, chunked, vectorized, and stored in the Astra table for usage.
After a PDF is written to AstraDB, it will be automatically deleted from the blob storage container.