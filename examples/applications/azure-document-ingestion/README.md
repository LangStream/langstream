
## Azure Blob Storage Ingestion
This sample application shows how to create a continuous PDF document ingestion pipeline that ingests documents from an Azure blob storage container.

This pipeline will:
- Create a Cassandra keyspace and table in AstraDB (if they haven't already been created)
- Detect new PDF files uploaded to the specified Azure blob storage container
- Extract text from the PDFs
- Chunk the PDF text with a Recursive Character Text Splitter
- Convert the chunks into a JSON structure
- Extract information from the filename
- Generate a vector embedding for the text contents
- Write the results to the AstraDB table

## Setup AstraDB
Follow the [AstraDB setup guide](https://docs.datastax.com/en/astra-serverless/docs/manage/db/manage-create.html) to create an AstraDB database.

## Deploy Dependencies

For production deployment, we recommend installing in Kubernetes, as mentioned [in our main doc](https://github.com/LangStream/langstream#production-ready-deployment).

However, for this example, we will use a docker deployment and will use Terraform to deploy our dependencies.

Modify this Terraform deployment script to suit your requirements and use it to deploy the Azure container and a VM for running Langstream.
. . .
Be sure to replace file("~/.ssh/id_rsa.pub") with the actual path to your public key file for SSH access to the VM.

The VM will need Docker ... ? 
If instead you already have an Azure container and runtime location you'd like to use, then you can skip this part for now since more info will be provided later in this guide.
### Upload files
Next, upload a set of PDF files to your new Azure container. This example assumes that the files follow the naming convention:
`"${productName} ${productVersion}.pdf"`, like:
`"appleWatch 7.12-v4.pdf"`
In this case, appleWatch will map to `productName`, and 7.12-v4 will map to `productVersion`.

## Setup secrets

Provide the credentials and details for your Azure container, OpenAI, and AstraDB.
If you already have an Azure blob storage container you'd like to use, you can find the connection info [here].
Additionally, you can create a storage access key by following this guide [here].

The Astra keyspace and table will be generated automatically when the pipeline is run.
So, the names specified in the secrets file will be used to generate these entities in AstraDB.

Warning:
Be careful to protect your secrets and never upload them to source control like Git.
(It is better to save them in a KMS and add any new files to a .gitignore immediately if they are part of a code project to prevent committing them.)

To avoid storing credentials in plain text, which is unsafe in production, we will use the Azure Key Vault to store the credentials for this tutorial. 
Although this is safer than storing credentials in plain text, always use caution when working with credentials, and never assume that a script is safe for production usage without performing your own security review first.

In the Terraform deployment script, update the variables at the top of the script to reflect your desired values.

Notice especially these values:

```commandline
# Storing the OpenAI access key in Azure Key Vault
resource "azurerm_key_vault_secret" "openai_access_key" {
  name         = "openai-access-key"
  value        = "example-key"
  key_vault_id = azurerm_key_vault.example.id
}

# Storing the AstraDB token in Azure Key Vault
resource "azurerm_key_vault_secret" "astra_token" {
  name         = "astra-token"
  value        = "AstraCS:example-token"
  key_vault_id = azurerm_key_vault.example.id
}

# Storing the storage access key in Azure Key Vault after it is generated
resource "azurerm_key_vault_secret" "storage_access_key" {
  name         = "storage-access-key"
  value        = azurerm_storage_account.example.primary_access_key
  key_vault_id = azurerm_key_vault.example.id
  depends_on   = [azurerm_storage_account.example] # Making sure the storage account is created first
}
```
You will need to specify the actual values for the first two after following the required steps to obtain an AstraDB token and an OpenAI access key.
Using an Azure OpenAI key is a supported use case, so that's what we will do here.

To simplify the setup for you, we are also using this Terraform script to provision a VM to run LangStream.
To avoid encouraging the storage of tokens in plain text, the setup script that is used to bootstrap the VM is using Azure Key Vault 
we are setup script to retrieve the tokens 

Here is an example secrets.yaml file:

```
secrets:
  - id: open-ai
    data:
      access-key: "\$openai_access_key"
      url: "https://example-domain.openai.azure.com"
      provider: "azure"
      embeddings-model: "text-embedding-ada-002"
      version: "2023-03-15-preview"
      llm: "gpt-4"
  - id: astra
    data:
      token: "\$astra_token"
      database: "example-database"
      environment: "prod"
      keyspace: "example_keyspace"
      table: "example_table"
  - id: azure
    data:
      storage-access-key: "\$storage_access_key"
      storage-account-name: "example-azure-storage-account-name"
      container: "example-azure-storage-container-name"
```
and update the variables at the top of the file 
replace ${CONTENTS_OF_PIPELINE_FILE} in the TF script with the contents of the pipeline.yaml file so that it appears like the example of secrets.yaml in the TF deployment script.



Ensure you've created a database in Astra, and use that database's name instead of "example-database" above.
Download the secure bundle and update the path in the secrets YAML to point to the directory where it will reside.

To run the flow, run these commands:
cd examples/applications/azure-document-ingestion
langstream docker run test -app . -s ../../secrets/secrets-azure-document-ingestion.yaml --langstream-runtime-docker-image public.ecr.aws/y3i6u2n7/datastax-public/langstream-runtime-tester --langstream-runtime-version latest

or:

langstream docker run test -app . -s ../../secrets/secrets-azure-document-ingestion.yaml
Ensure that any sensitive files are added to .gitignore

Any PDF files that are added to the Azure blob storage container will be automatically ingested into the flow, chunked, vectorized, and stored in the Astra table for usage.
After a PDF is written to AstraDB, it will be automatically deleted from the blob storage container.

The deployment.tf file can be used to deploy an example Azure blob storage container and VM that can reach it.
(That VM can be used to run langstream.)