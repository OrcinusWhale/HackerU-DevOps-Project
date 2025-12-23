variable "region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "eu-central-1"
}

variable "ami_id" {
  description = "The AMI ID for the EC2 instance"
  type        = string
  default     = "ami-015f3aa67b494b27e" 
}

variable "instance_type" {
  description = "The type of EC2 instance to use"
  type        = string
  default     = "m7i-flex.large"
}

variable "key_name" {
  description = "The name of the SSH key pair"
  type        = string
  default     = "AWS-eu-central-1"
}