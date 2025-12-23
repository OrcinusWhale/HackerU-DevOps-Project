terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "random_password" "k3s_token" {
  length  = 48
  special = false
}

resource "aws_security_group" "main" {
  name = "main_security_group"
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "kubernetes_master" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.main.id]
  user_data              = <<-EOF
                #!/bin/bash
                curl -sfL https://get.k3s.io | K3S_TOKEN=${random_password.k3s_token.result} sh -
                EOF

  tags = {
    Name = "KubernetesMaster"
  }
}

resource "aws_instance" "kubernetes_worker" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.main.id]
  user_data = <<-EOF
                #!/bin/bash
                exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
                MASTER_IP="${aws_instance.kubernetes_master.private_ip}"
                
                # Wait for Master API to be reachable
                while ! curl -k --output /dev/null --silent --head "https://$MASTER_IP:6443"; do
                  echo "Waiting for Master API server at $MASTER_IP:6443..."
                  sleep 5
                done

                curl -sfL https://get.k3s.io | K3S_URL="https://$MASTER_IP:6443" K3S_TOKEN=${random_password.k3s_token.result} sh -
                EOF
  tags = {
    Name = "KubernetesWorker"
  }
  depends_on = [aws_instance.kubernetes_master]
}
