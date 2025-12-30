variable "aws_region" {
  type    = string
  default = "eu-central-1"
}

variable "project_name" {
  type    = string
  default = "devops-final"
}

variable "master_instance_type" {
  type    = string
  default = "m7i-flex.large"
}

variable "worker_instance_type" {
  type    = string
  default = "m7i-flex.large"
}

variable "k8s_node_count" {
  type    = number
  default = 2
}

variable "bridge_nodeport" {
  type    = number
  default = 30082
}

variable "kibana_nodeport" {
  type    = number
  default = 30601
}

variable "elasticsearch_nodeport" {
  type    = number
  default = 30920
}

variable "data_prefix" {
  type    = string
  default = "ingest/"
}

variable "ssh_key_name" {
  type    = string
  default = ""
}

variable "ssh_allowed_cidr" {
  type        = string
  default     = "0.0.0.0/0"
  description = "CIDR allowed to SSH and access NodePorts (e.g. your public IP/32)."
}

variable "deploy_platform" {
  type    = bool
  default = true
}

