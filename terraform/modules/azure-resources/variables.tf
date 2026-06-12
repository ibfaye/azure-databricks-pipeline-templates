variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "westeurope"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    managed_by  = "terraform"
    project     = "databricks-pipeline"
  }
}

variable "vnet_address_space" {
  description = "Address space for the VNet"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "subnet_prefixes" {
  description = "Subnet prefixes"
  type = object({
    public  = list(string)
    private = list(string)
  })
  default = {
    public  = ["10.0.1.0/24"]
    private = ["10.0.2.0/24"]
  }
}
