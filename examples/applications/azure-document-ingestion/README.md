
## FYI: This Readme is a Work in progress. 

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

## Setting Up AstraDB

In this part of the guide, we will guide you through the steps to create a new AstraDB database instance. AstraDB by DataStax is a scalable and globally distributed cloud database built on Apache Cassandra™.

### Prerequisites

- A DataStax Astra account. If you don't have one, you can create a [free tier account](https://astra.datastax.com/register).

### Steps to Create an AstraDB Database

#### 1. Sign in to DataStax Astra

Navigate to [DataStax Astra](https://astra.datastax.com/) and log in with your account credentials.

#### 2. Create a New Database

- Click the `Databases` tab on the sidebar or navigate to the `Dashboard`.
- Click the `Create Database` button.

#### 3. Configure Your Database

- **Database Name**: Enter a unique name for your database.
- **Keyspace Name**: Enter a name for your keyspace.
- **Cloud Provider**: Select your preferred cloud provider. AstraDB supports AWS, GCP, and Azure. In this case, we will select **Azure**.
- **Region**: Choose the region closest to your application or users.

Click `Create Database`.

#### 4. Wait for Your Database to Spin Up

It may take a few minutes for your database to be ready. You can see the status on the dashboard.

#### 5. Connect to Your Database

Once your database is ready, click `Connect` to find connection details and credentials. You can connect using various methods like CQLSH, SDKs, or REST API.

### 6. Create a Token

To interact with your database programmatically, you'll need a token. Follow these steps to generate one:

- Navigate to the `Settings` tab on the sidebar in the Astra DB console.
- Click on `Tokens`.
- Click the `Generate Token` button.
- Choose the role for the token. For instance, choose `Database Administrator` for full access to managing databases. We recommend getting things working first and then locking down permissions to a role with more restricted access levels that are more appropriate for production usage.
- Click `Generate Token`.

**Important**: Save the token in a secure location. You will not be able to view it again.

You will also receive a `Client ID` and `Client Secret`. Keep these credentials secure as well.


For more instructions, follow the [AstraDB setup guide](https://docs.datastax.com/en/astra-serverless/docs/manage/db/manage-create.html) to create an AstraDB database.


## Creating an Azure Blob Storage Container

In this part of the guide, we will walk you through the steps to create a Blob storage container on Microsoft Azure. Blob storage is optimized for storing massive amounts of unstructured data, such as text or binary data.

### Prerequisites

- An active Azure account. If you don't have one, you can create a [free account](https://azure.com/free).
- Azure Storage account. Follow this [guide](https://docs.microsoft.com/azure/storage/common/storage-account-create) to create a storage account.

### Steps to Create a Blob Storage Container in the UI (Optional if using Terraform later in this guide)

#### 1. Sign in to Azure Portal

Navigate to [Azure Portal](https://portal.azure.com/) and log in with your Azure account credentials.

#### 2. Navigate to your Storage Account

- In the left sidebar, click on "Storage accounts".
- Click on the name of your storage account, e.g. `<STORAGE_ACCOUNT_NAME>`.

#### 3. Create a Blob Container

- Within your storage account page, look for the `Blob service` section in the left sidebar.
- Click on `Containers`.
- Click the `+ Container` button at the top of the page.
- In the `Name` field, input a name for your container, e.g. `<CONTAINER_NAME>`.
- Choose the `Public access level`. It’s set to "Private (no anonymous access)" by default. You can change this based on your requirements. But, for this demo, we will assume that public is okay. Be sure to lock it down for production!
- Click `Create`.

Your Blob storage container is now created and ready to use!

## Deploy Dependencies, including provisioning the container via Terraform (Option 2)

**NOTE:**
For production deployment, we recommend installing in Kubernetes, as mentioned [in our main doc](https://github.com/LangStream/langstream#production-ready-deployment).

However, for this example, we will use a docker deployment and will use Terraform to deploy our dependencies.

Modify [this Terraform deployment script](deployment.tf) to suit your requirements and use it to deploy the Azure container and a VM for running Langstream.
Be sure to replace file("~/.ssh/id_rsa.pub") with the actual path to your public key file for SSH access to the VM.
This script will also provision all the required dependencies for running LangStream.

### Upload files
Next, upload a set of PDF files to your new Azure container. This example assumes that the files follow the naming convention:
`"${productName} ${productVersion}.pdf"`, like:
`"appleWatch 7.12-v4.pdf"`
In this case, appleWatch will map to `productName`, and 7.12-v4 will map to `productVersion`.

#### Uploading Files to the Blob Container

You can upload files to the container either through the Azure Portal or programmatically. Below is how you can do it through the Azure Portal:

1. Click on the name of your container.
2. Click the `Upload` button.
3. Browse and select files from your computer that you want to upload.
4. Click the `Upload` button.

#### Accessing the Blob Container Programmatically

You can access Blob containers programmatically using Azure SDKs available for different programming languages. 
However, programmatic access to Azure blob storage is out of scope of this tutorial on LangStream.

## Setup secrets

Warning:
Be careful to protect your secrets and never upload them to source control like Git.
(It is better to save them in a KMS and add any new files to a .gitignore immediately if they are part of a code project to prevent committing them.)

### Using Azure Key Vault
To avoid storing credentials in plain text, which is unsafe in production, we will use the Azure Key Vault to store the credentials for this tutorial. 
Although this is safer than storing credentials in plain text, always use caution when working with credentials, and never assume that a script is safe for production usage without performing your own security review first.

#### Azure Storage access key
Azure provides additional authentication options, but for this tutorial, we will use the storage access key method.
You can create a storage access key by following [this guide](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).

#### Azure OpenAI API token
Instructions for obtaining an API token are available from Azure. 
#### Non-Azure OpenAI approach (alternative option)
If you want to use OpenAI (not Azure's OpenAI), you can find instructions on configuring an OpenAI API Key [in their guide](https://platform.openai.com/docs/quickstart?context=python). 

#### Other LLMs/Embeddings (alternative option)
If you decide to swap out the LLM and embedding model for a different method, please create a Github issue in LangStream to request your specific use case.


Provide the credentials and details for your Azure container, OpenAI, and AstraDB.
If you already have an Azure blob storage container you'd like to use, you can find the connection info [here].

#### Note on AstraDB keyspace and table
The Astra keyspace and table will be generated automatically when the LangStream pipeline is run.
So, the names specified in the [secrets file](../../secrets/secrets.yaml) will be used to generate these entities in AstraDB.

### Specify the secrets file
For this tutorial, if you are running locally, we assume that you will update this [secrets file](../../secrets/secrets.yaml) to specify your specific secrets and configurations.
If you will be running the Terraform script to deploy resources into your Azure environment, then you will need to update the secrets section of the [Terraform script](deployment.tf) instead.
#### Running locally
For running locally, here is an example secrets.yaml file that will work for this tutorial after you provide your specific values:

```
secrets:
  - id: open-ai
    data:
      access-key: "example_key"
      url: "https://example-domain.openai.azure.com"
      embeddings-model: "text-embedding-ada-002"
      version: "2023-03-15-preview"
      llm: "gpt-4"
  - id: astra
    data:
      token: "AstraCS:exampleToken"
      database: "example-database"
      environment: "prod"
      keyspace: "example_keyspace"
      table: "example_table"
  - id: azure
    data:
      storage-access-key: "example_storage_key"
      storage-account-name: "example-azure-storage-account-name"
      container: "example-azure-storage-container-name"
```
Be sure to substitute your specific tokens and names.
#### Running via Terraform
In the [Terraform deployment script](deployment.tf), update the variables at the top of the script to reflect your desired values.

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
```
You will need to specify the actual values instead of `example-key` and `AstraCS:example-token`.
These values will be used later in the script to generate a secrets.yaml file by using Azure Key Vault.
This secrets.yaml file will run within a VM provisioned by Terraform, and the file will be generated when the VM is bootstrapped.
To prevent the secrets from being emitted into logs in plain text, we are using Azure Key Vault.
If you change the vault name from `examplevault`, make sure that the corresponding lines of the bootstrap script are also updated.
For example, the lines like `openai_access_key=\$(az keyvault secret show --name openai-access-key --vault-name examplevault --query value -o tsv)` in [the deployment script](deployment.tf) will need to reference the new vault.
Make sure that any other variables are updated in the script to reflect your current environment and deployment configurations.

### Systemd service (to do)

## Running the flow locally:

There are multiple ways to run LangStream, such as with Kubernetes, minikube, etc.
For this guide, we will use Docker for simplicity. 

### Installing LangStream
To install LangStream locally, please follow [these instructions](https://github.com/LangStream/langstream#installation)

### Starting the pipeline

To run the flow, make sure your secrets file has been updated.
Then, make sure you've added at least one PDF to the container. Make sure its file name matches the naming convention specified earlier in this guide.
Then, run these commands:
```
cd examples/applications/azure-document-ingestion
langstream docker run test -app . -s ../../secrets/secrets.yaml
```

Any PDF files that are added to the Azure blob storage container will be automatically ingested into the flow, chunked, vectorized, and stored in the Astra table for usage.
After a PDF is written to AstraDB, it will be automatically deleted from the blob storage container.
