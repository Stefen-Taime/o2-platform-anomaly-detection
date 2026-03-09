variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
}

variable "my_ip" {
  description = "Your public IP for SSH/UI access (CIDR format)"
  type        = string
}
