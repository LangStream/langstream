locals {
  pipeline_content = file("${path.module}/pipeline.yaml")
}
# Note: Replace "${path.module}/pipeline.yaml" with the actual relative path to your pipeline.yaml file.
provider "azurerm" {
  features {}
}

data "azurerm_client_config" "example" {}

resource "azurerm_resource_group" "example" {
  name     = "example-resources"
  location = "East US"
}

resource "azurerm_storage_account" "example" {
  name                     = "examplestoracc"
  resource_group_name      = azurerm_resource_group.example.name
  location                 = azurerm_resource_group.example.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "example" {
  name                  = "content"
  storage_account_name  = azurerm_storage_account.example.name
  container_access_type = "private"
}

resource "azurerm_key_vault" "example" {
  name                        = "examplevault"
  location                    = azurerm_resource_group.example.location
  resource_group_name         = azurerm_resource_group.example.name
  tenant_id                   = data.azurerm_client_config.example.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 7
}

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

resource "azurerm_virtual_network" "example" {
  name                = "example-network"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "example" {
  name                 = "internal"
  resource_group_name  = azurerm_resource_group.example.name
  virtual_network_name = azurerm_virtual_network.example.name
  address_prefixes     = ["10.0.2.0/24"]
}

resource "azurerm_public_ip" "example" {
  name                = "example-pip"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location
  allocation_method   = "Dynamic"
}

resource "azurerm_network_security_group" "example" {
  name                = "example-nsg"
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
}

resource "azurerm_network_interface" "example" {
  name                = "example-nic"
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.example.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.example.id
  }
}

resource "azurerm_linux_virtual_machine" "example" {
  name                = "example-vm"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location
  size                = "Standard_DS1_v2"
  admin_username      = "adminuser"
  network_interface_ids = [
    azurerm_network_interface.example.id,
  ]

  admin_ssh_key {
    username   = "adminuser"
    public_key = file("~/.ssh/id_rsa.pub")
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }

  identity {
    type = "SystemAssigned"
  }

  # Since we are using the Azure KMS to retrieve the keys, we need to ensure the appropriate libraries are installed in the script to access them.
    custom_data = base64encode(<<-EOT
              #!/bin/bash
              apt-get update -y
              apt-get install -y apt-transport-https ca-certificates curl software-properties-common git jq lsb-release

              # Install Azure CLI
              curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-archive-keyring.gpg
              echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/microsoft-archive-keyring.gpg] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" | \
              tee /etc/apt/sources.list.d/azure-cli.list > /dev/null
              apt-get update
              apt-get install azure-cli -y

              # Install Docker
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
              add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
              apt-get update -y
              apt-get install -y docker-ce docker-ce-cli containerd.io
              systemctl start docker
              systemctl enable docker

              # Install the LangStream binary:
              curl -Ls "https://raw.githubusercontent.com/LangStream/langstream/main/bin/get-cli.sh" | bash
              chmod +x /usr/local/bin/langstream

              # Retrieve secrets from Key Vault
              openai_access_key=\$(az keyvault secret show --name openai-access-key --vault-name examplevault --query value -o tsv)
              astra_token=\$(az keyvault secret show --name astra-token --vault-name examplevault --query value -o tsv)
              storage_access_key=\$(az keyvault secret show --name storage-access-key --vault-name examplevault --query value -o tsv)

              # Create the secrets.yaml file in a secure location
              cat > /root/secrets.yaml <<EOF
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
EOF

              chmod 600 /root/secrets.yaml  # Change the file permission to be readable only by the root

              cat > /root/pipeline.yaml <<EOF
${local.pipeline_content}
EOF
              EOT
  )
}

data "azurerm_builtin_role_definition" "reader" {
  name = "Storage Blob Data Reader"
}

data "azurerm_builtin_role_definition" "contributor" {
  name = "Storage Blob Data Contributor"
}
data "azurerm_builtin_role_definition" "keyvault_reader" {
  name = "Key Vault Secrets User"
}

resource "azurerm_role_assignment" "reader" {
  principal_id   = azurerm_linux_virtual_machine.example.identity[0].principal_id
  role_definition_id = data.azurerm_builtin_role_definition.reader.id
  scope           = azurerm_storage_account.example.id
}

resource "azurerm_role_assignment" "contributor" {
  principal_id   = azurerm_linux_virtual_machine.example.identity[0].principal_id
  role_definition_id = data.azurerm_builtin_role_definition.contributor.id
  scope           = azurerm_storage_account.example.id
}


resource "azurerm_role_assignment" "keyvault_reader" {
  principal_id       = azurerm_linux_virtual_machine.example.identity[0].principal_id
  role_definition_id = data.azurerm_builtin_role_definition.keyvault_reader.id
  scope              = azurerm_key_vault.example.id
}