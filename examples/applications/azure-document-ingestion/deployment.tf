terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.77.0"
    }
  }
}

locals {
  secret_content = file("${path.module}/../../secrets/secrets.yaml")
  ignore_content = file("${path.module}/.langstreamignore")
  assets_content = file("${path.module}/assets.yaml")
  chatbot_content = file("${path.module}/chatbot.yaml")
  config_content = file("${path.module}/configuration.yaml")
  gateway_content = file("${path.module}/gateways.yaml")
  pipeline_content = file("${path.module}/pipeline.yaml")
  python_content = file("${path.module}/python/langchain_chat.py")
}
# Note: Replace "${path.module}/pipeline.yaml" with the actual relative path to your pipeline.yaml file.
provider "azurerm" {
  features {}
}

data "azurerm_client_config" "example" {}

resource "azurerm_resource_group" "example" {
  name     = "example-resources"
  location = "Central US"
}

resource "azurerm_storage_account" "example" {
  name                     = "examplestoracc124"
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

resource "azurerm_network_security_rule" "ssh_access" {
  name                        = "SSHAccess"
  priority                    = 1001
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "22"
  source_address_prefix       = "76.199.19.13" # Change to match your IP
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.example.name
  network_security_group_name = azurerm_network_security_group.example.name
}

resource "azurerm_network_interface_security_group_association" "example" {
  network_interface_id      = azurerm_network_interface.example.id
  network_security_group_id = azurerm_network_security_group.example.id
}

resource "azurerm_linux_virtual_machine" "example" {
  name                = "example-vm"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location
  size                = "Standard_DS3_v2"
  admin_username      = "adminuser"
  network_interface_ids = [
    azurerm_network_interface.example.id,
  ]

  admin_ssh_key {
    username   = "adminuser"
    public_key = file("~/.ssh/azure.pub")
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

  custom_data = base64encode(<<-EOT
    #!/bin/bash
    cat > /root/secrets.yaml <<SECRETS
    ${local.secret_content}
    SECRETS

    cat > /root/.langstreamignore <<IGNORE
    ${local.ignore_content}
    IGNORE
    
    cat > /root/assets.yaml <<ASSETS
    ${local.assets_content}
    ASSETS

    cat > /root/chatbot.yaml <<CHATBOT
    ${local.chatbot_content}
    CHATBOT

    cat > /root/configuration.yaml <<CONFIGURATION
    ${local.config_content}
    CONFIGURATION

    cat > /root/gateways.yaml <<GATEWAYS
    ${local.gateway_content}
    GATEWAYS

    cat > /root/pipeline.yaml <<PIPELINE
    ${local.pipeline_content}
    PIPELINE

    mkdir /root/python

    cat > /root/python/langchain_chat.py <<PYTHON
    ${local.python_content}
    PYTHON

    apt-get update -y
    apt-get install -y apt-transport-https ca-certificates curl software-properties-common git jq lsb-release unzip openjdk-17-jre openjdk-17-jdk

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
    usermod -aG docker adminuser
    systemctl start docker
    systemctl enable docker

    # Create a new user for running the application
    useradd -m -s /bin/bash adminuser

    # Install the LangStream binary:
    runuser -l adminuser -c 'curl -Ls "https://raw.githubusercontent.com/LangStream/langstream/main/bin/get-cli.sh" | bash'

    runuser -l adminuser -c 'source /home/adminuser/.bashrc'

    # Move and change ownership of the secrets and pipeline files to the new user
    mv /root/secrets.yaml /home/adminuser/
    mkdir /home/adminuser/app
    mv /root/pipeline.yaml /home/adminuser/app/
    mv /root/configuration.yaml /home/adminuser/app/
    chown adminuser:adminuser /home/adminuser/secrets.yaml
    chown -R adminuser:adminuser /home/adminuser/app/

    runuser -l adminuser -c 'chmod 600 /home/adminuser/secrets.yaml'  # Change the file permission to be readable only by the owner

    # Create a systemd service file to run the application
    cat > /etc/systemd/system/myapp.service <<EOF
    [Unit]
    Description=My Application Service
    After=network.target

    [Service]
    User=adminuser
    WorkingDirectory=/home/adminuser/app
    ExecStart=/home/adminuser/.langstream/candidates/current/bin/langstream docker run test -app /home/adminuser/app -s /home/adminuser/secrets.yaml
    Restart=always

    [Install]
    WantedBy=multi-user.target
    EOF
    # Reload systemd, enable and start the service
    systemctl daemon-reload
    systemctl enable myapp.service
    systemctl start myapp.service

    EOT
  )
}

data "azurerm_role_definition" "reader" {
  name = "Storage Blob Data Reader"
}

data "azurerm_role_definition" "contributor" {
  name = "Storage Blob Data Contributor"
}

resource "azurerm_role_assignment" "reader" {
  principal_id   = azurerm_linux_virtual_machine.example.identity[0].principal_id
  role_definition_id = data.azurerm_role_definition.reader.id
  scope           = azurerm_storage_account.example.id
}

resource "azurerm_role_assignment" "contributor" {
  principal_id   = azurerm_linux_virtual_machine.example.identity[0].principal_id
  role_definition_id = data.azurerm_role_definition.contributor.id
  scope           = azurerm_storage_account.example.id
}