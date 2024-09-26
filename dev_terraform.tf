# Configure the Azure Provider
provider "azurerm" {
  features {}

  # Placeholder for SPN authentication
  subscription_id = "<<placeholder>>"
  client_id       = "<<placeholder>>"
  client_secret   = "<<placeholder>>"
  tenant_id       = "<<placeholder>>"
}

# Resource Group
resource "azurerm_resource_group" "pfg-terraform" {
  name     = "pfg-terraform"
  location = "West US 2"
}

# Virtual Network for Private Links
resource "azurerm_virtual_network" "pfg-terraform-vnet" {
  name                = "pfg-terraform-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.pfg-terraform.location
  resource_group_name = azurerm_resource_group.pfg-terraform.name
}

# Subnet for Private Endpoints

resource "azurerm_subnet" "pfg-terraform-subnet" {
  name                 = "pfg-terraform-subnet"
  resource_group_name  = azurerm_resource_group.pfg-terraform.name
  virtual_network_name = azurerm_virtual_network.pfg-terraform-vnet.name
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
resource "azurerm_key_vault" "pfg-terraform-kv" {
  name                = "pfg-terraform-keyvault"
  location            = azurerm_resource_group.pfg-terraform.location
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  tenant_id           = "86fb359e-1360-4ab3-b90d-2a68e8c007b9"
  sku_name            = "standard"

  network_acls {
    default_action             = "Deny"
    bypass                     = "AzureServices"
    virtual_network_subnet_ids = [azurerm_subnet.pfg-terraform-subnet.id]
  }
}

# Private DNS Zone for PostgreSQL
resource "azurerm_private_dns_zone" "pfg-terraform-private-dns-zone" {
  name                = "privatelink.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.pfg-terraform.name
}

resource "azurerm_private_dns_zone_virtual_network_link" "pfg-terraform-private-dns-zone-link" {
  name                  = "pfg-terraform-private-dns-zone-link"
  resource_group_name   = azurerm_resource_group.pfg-terraform.name
  virtual_network_id    = azurerm_virtual_network.pfg-terraform-vnet.id
  private_dns_zone_name = azurerm_private_dns_zone.pfg-terraform-private-dns-zone.name
}

# PostgreSQL Flexible Server
resource "azurerm_postgresql_flexible_server" "pfg_terraform_postgres" {
  name                = "pfg-terraform-postgres"
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  location            = azurerm_resource_group.pfg-terraform.location
  version             = "16"
  delegated_subnet_id = azurerm_subnet.pfg-terraform-subnet.id
  private_dns_zone_id = azurerm_private_dns_zone.pfg-terraform-private-dns-zone.id

  administrator_login    = "<<placeholder>>"
  administrator_password = "<<placeholder>>"

  sku_name   = "GP_Standard_D2s_v3"
  storage_mb = 32768

  backup_retention_days         = 7
  geo_redundant_backup_enabled  = false
  public_network_access_enabled = false # Disable public network access
}

# Private Endpoint for PostgreSQL
resource "azurerm_private_endpoint" "pfg_terraform_postgres_pe" {
  name                = "pfg-terraform-postgres-private-endpoint"
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  location            = azurerm_resource_group.pfg-terraform.location
  subnet_id           = azurerm_subnet.pfg-terraform-subnet.id

  private_service_connection {
    name                           = "postgresConnection"
    private_connection_resource_id = azurerm_postgresql_flexible_server.pfg_terraform_postgres.id
    subresource_names              = ["postgresqlServer"]
    is_manual_connection           = false
  }
}

# Static Web App 1
resource "azurerm_static_web_app" "pfg-ap-portal" {
  name                = "pfg-ap-portal"
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  location            = azurerm_resource_group.pfg-terraform.location
  sku_tier            = "Standard"

  identity {
    type = "SystemAssigned"
  }
}

# Static Web App 2
resource "azurerm_static_web_app" "pfg-admin-portal" {
  name                = "pfg-admin-portal"
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  location            = azurerm_resource_group.pfg-terraform.location
  sku_tier            = "Standard"

  identity {
    type = "SystemAssigned"
  }
}

# Private Endpoint for Static Web App 1
resource "azurerm_private_endpoint" "pfg_terraform_pe_site_1" {
  name                = "pfg-terraform-pe-site-1"
  location            = azurerm_resource_group.pfg-terraform.location
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  subnet_id           = azurerm_subnet.pfg-terraform-subnet.id

  private_service_connection {
    name                           = "privatelink-staticwebapp1"
    private_connection_resource_id = azurerm_static_web_app.pfg-ap-portal.id
    subresource_names              = ["staticSites"]
    is_manual_connection           = false
  }
}

# Private Endpoint for Static Web App 2
resource "azurerm_private_endpoint" "pfg_terraform_pe_site_2" {
  name                = "pfg-terraform-pe-site-2"
  location            = azurerm_resource_group.pfg-terraform.location
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  subnet_id           = azurerm_subnet.pfg-terraform-subnet.id

  private_service_connection {
    name                           = "privatelink-staticwebapp2"
    private_connection_resource_id = azurerm_static_web_app.pfg-admin-portal.id
    subresource_names              = ["staticSites"]
    is_manual_connection           = false
  }
}

# App Service Plan
resource "azurerm_service_plan" "pfg-terraform-asp" {
  name                = "pfg-terraform-asp"
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  location            = azurerm_resource_group.pfg-terraform.location
  os_type             = "Linux"
  sku_name            = "P1v2"
}

# Define the Application Insights resource
resource "azurerm_application_insights" "pfg_terraform_app_insights" {
  name                = "pfg-terraform-app-insights"
  location            = azurerm_resource_group.pfg-terraform.location
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  application_type    = "web"

  tags = {
    environment = "development"
  }
}

# App Service
resource "azurerm_linux_web_app" "pfg-terraform-app" {
  name                = "pfg-terraform-app"
  resource_group_name = azurerm_resource_group.pfg-terraform.name
  location            = azurerm_resource_group.pfg-terraform.location

  service_plan_id = azurerm_service_plan.pfg-terraform-asp.id
  identity {
    type = "SystemAssigned"
  }

  site_config {

  }

  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "APPINSIGHTS_INSTRUMENTATIONKEY"      = azurerm_application_insights.pfg_terraform_app_insights.instrumentation_key
  }

  tags = {
    environment = "development"
  }
}
