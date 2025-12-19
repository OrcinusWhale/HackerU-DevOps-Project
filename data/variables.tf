variable "aws_region" {
  type    = string
  default = "eu-central-1"
}

variable "project_name" {
  type    = string
  default = "devops-final"
}

variable "instance_type" {
  type    = string
  default = "t3.micro"
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

variable "ssh_ingress_cidr" {
  type        = string
  default     = "0.0.0.0/0"
  description = "CIDR allowed to SSH to the EC2 instances (e.g. your public IP/32). Leave empty to disable SSH ingress rule."
}
