# Configure the Azure Provider
provider "azurerm" {
  features {}

  subscription_id = "<<placeholder>>"
  client_id       = "<<placeholder>>"
  client_secret   = "<<placeholder>>"
  tenant_id       = "<<placeholder>>"
}

# Resource Group
resource "azurerm_resource_group" "rg-apportal-dev-westus2-001" {
  name     = "rg-apportal-dev-westus2-001"
  location = "West US 2"
  tags = {
    environment = "dev",
    owner = "",
    project = "",
    department = ""
  }
}

# Virtual Network for Private Links
resource "azurerm_virtual_network" "vnet-apportal-dev-westus2-001" {
  name                = "vnet-apportal-dev-westus2-001"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

# Subnet for Private Endpoints
resource "azurerm_subnet" "sub-apportal-dev-westus2-001" {
  name                 = "sub-apportal-dev-westus2-001"
  resource_group_name  = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  virtual_network_name = azurerm_virtual_network.vnet-apportal-dev-westus2-001.name
  address_prefixes     = ["10.0.1.0/24"]

  service_endpoints = [
    "Microsoft.KeyVault"
  ]

  delegation {
    name = "postgresqlDelegation"
    service_delegation {
      name = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/action"
      ]
    }
  }
}

# Key Vault
resource "azurerm_key_vault" "kv-apportal-dev-westus2-001" {
  name                = "kvapportaldevwestus2001"
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  tenant_id           = <<placeholder>>
  sku_name            = "standard"

  network_acls {
    default_action             = "Deny"
    bypass                     = "AzureServices"
    virtual_network_subnet_ids = [azurerm_subnet.sub-apportal-dev-westus2-001.id]
  }
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

# Private DNS Zone for PostgreSQL
resource "azurerm_private_dns_zone" "dns-apportal-dev-westus2-001" {
  name                = "privatelink.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

resource "azurerm_private_dns_zone_virtual_network_link" "dns-link-apportal-dev-westus2-001" {
  name                  = "dns-link-apportal-dev-westus2-001"
  resource_group_name   = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  virtual_network_id    = azurerm_virtual_network.vnet-apportal-dev-westus2-001.id
  private_dns_zone_name = azurerm_private_dns_zone.dns-apportal-dev-westus2-001.name
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

# PostgreSQL Flexible Server
resource "azurerm_postgresql_flexible_server" "pg-apportal-dev-westus2-001" {
  name                = "pg-apportal-dev-westus2-001"
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  version             = "16"
  delegated_subnet_id = azurerm_subnet.sub-apportal-dev-westus2-001.id
  private_dns_zone_id = azurerm_private_dns_zone.dns-apportal-dev-westus2-001.id

  administrator_login    = <<placeholder>>
  administrator_password = <<placeholder>>

  sku_name   = "GP_Standard_D2s_v3"
  storage_mb = 32768

  backup_retention_days         = 7
  geo_redundant_backup_enabled  = false
  public_network_access_enabled = false # Disable public network access
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

# Static Web App 1
resource "azurerm_static_web_app" "app-apportal-dev-westus2-001" {
  name                = "app-apportal-dev-westus2-001"
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  sku_tier            = "Standard"
  sku_size            = "Standard"
  identity {
    type = "SystemAssigned"
  }
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

# Static Web App 2
resource "azurerm_static_web_app" "app-apportal-dev-westus2-002" {
  name                = "app-apportal-dev-westus2-002"
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  sku_tier            = "Standard"
  sku_size            = "Standard"
  identity {
    type = "SystemAssigned"
  }
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}


# Private Endpoint for PostgreSQL
resource "azurerm_private_endpoint" "pe-pg-apportal-dev-westus2-001" {
  name                = "pe-pg-apportal-dev-westus2-001"
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  subnet_id           = azurerm_subnet.sub-apportal-dev-westus2-001.id

  private_service_connection {
    name                           = "postgresConnection"
    private_connection_resource_id = azurerm_postgresql_flexible_server.pg-apportal-dev-westus2-001.id
    subresource_names              = ["postgresqlServer"]
    is_manual_connection           = false
  }
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}


# Private Endpoint for Static Web App 1
resource "azurerm_private_endpoint" "pe-app1-apportal-dev-westus2-001" {
  name                = "pe-app1-apportal-dev-westus2-001"
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  subnet_id           = azurerm_subnet.sub-apportal-dev-westus2-001.id

  private_service_connection {
    name                           = "privatelink-staticwebapp1"
    private_connection_resource_id = azurerm_static_web_app.app-apportal-dev-westus2-001.id
    subresource_names              = ["staticSites"]
    is_manual_connection           = false
  }
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

# Private Endpoint for Static Web App 2
resource "azurerm_private_endpoint" "pe-app2-apportal-dev-westus2-002" {
  name                = "pe-app2-apportal-dev-westus2-002"
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  subnet_id           = azurerm_subnet.sub-apportal-dev-westus2-001.id

  private_service_connection {
    name                           = "privatelink-staticwebapp2"
    private_connection_resource_id = azurerm_static_web_app.app-apportal-dev-westus2-002.id
    subresource_names              = ["staticSites"]
    is_manual_connection           = false
  }
  tags = {
    environment = "development",
    owner = "",
    project = "",
    department = ""
  }
}

# App Service Plan
resource "azurerm_service_plan" "asp-apportal-dev-westus2-001" {
  name                = "asp-apportal-dev-westus2-001"
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  os_type             = "Linux"
  sku_name = "B1"
  tags = {
    environment = "development",
    owner       = "",
    project     = "",
    department  = ""
  }
}

# Application Insights
resource "azurerm_application_insights" "ai-apportal-dev-westus2-001" {
  name                = "ai-apportal-dev-westus2-001"
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  application_type    = "web"

  tags = {
    environment = "development",
    owner       = "",
    project     = "",
    department  = ""
  }
}


# App Service 1
resource "azurerm_linux_web_app" "app-apportal-dev-westus2-001" {
  name                = "app-apportal-dev-westus2-001"
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  service_plan_id     = azurerm_service_plan.asp-apportal-dev-westus2-001.id

  site_config {
  }

  identity {
    type = "SystemAssigned"
  }

  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "APPINSIGHTS_INSTRUMENTATIONKEY"      = azurerm_application_insights.ai-apportal-dev-westus2-001.instrumentation_key
  }

  tags = {
    environment = "development",
    owner       = "",
    project     = "",
    department  = ""
  }
}

# Storage Account
resource "azurerm_storage_account" "sa-apportal-dev-westus2-001" {
  name                     = "saapportaldevwestus2001"  # Storage Account names must be globally unique and without hyphens.
  resource_group_name       = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  location                  = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  account_tier              = "Standard"
  account_replication_type  = "ZRS"
  account_kind              = "StorageV2"  # Specifies the type of storage account
  # Enabling Data Lake Storage Gen2
  is_hns_enabled = true     # Enable for hierarchical namespace (Data Lake Gen2)


  tags = {
    environment = "development"
    owner       = ""
    project     = ""
    department  = ""
  }
}

# Function App
resource "azurerm_function_app" "func-apportal-dev-westus2-001" {
  name                = "func-apportal-dev-westus2-001"
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  app_service_plan_id     = azurerm_service_plan.asp-apportal-dev-westus2-001.id

  storage_account_name       = azurerm_storage_account.sa-apportal-dev-westus2-001.name
  storage_account_access_key = azurerm_storage_account.sa-apportal-dev-westus2-001.primary_access_key

  os_type = "linux"
  version = "~4" # Specifies the version of the runtime. "~4" refers to the latest v4 runtime.
  https_only = true

  app_settings = {
    "FUNCTIONS_WORKER_RUNTIME" = "python"  # Assuming Python runtime
    "WEBSITE_RUN_FROM_PACKAGE" = "1"
    "APPINSIGHTS_INSTRUMENTATIONKEY" = azurerm_application_insights.ai-apportal-dev-westus2-001.instrumentation_key
  }

  identity {
    type = "SystemAssigned"
  }

  tags = {
    environment = "development"
    owner       = ""
    project     = ""
    department  = ""
  }
}

# Azure Form Recognizer
resource "azurerm_cognitive_account" "form_recognizer" {
  name                = "formrecognizer-apportal-dev-westus2-001"
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  sku {
    name     = "S1"
    capacity = 1
  }
  kind = "FormRecognizer"

  tags = {
    environment = "development"
    owner       = ""
    project     = ""
    department  = ""
  }
}

# Azure OpenAI
resource "azurerm_openai_service" "openai_service" {
  name                = "openai-apportal-dev-westus2-001"
  resource_group_name = azurerm_resource_group.rg-apportal-dev-westus2-001.name
  location            = azurerm_resource_group.rg-apportal-dev-westus2-001.location
  sku {
    name     = "S0"
    capacity = 1
  }
  kind = "OpenAI"

  tags = {
    environment = "development"
    owner       = ""
    project     = ""
    department  = ""
  }
}