terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ----------------------------
# Use default VPC/Subnets/SG
# ----------------------------
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_security_group" "default" {
  name   = "default"
  vpc_id = data.aws_vpc.default.id
}

resource "aws_security_group" "project" {
  name        = "${var.project_name}-sg"
  description = "Security group for ${var.project_name} (k3s + lambdas)"
  vpc_id      = data.aws_vpc.default.id

  # Allow all traffic within the same SG (keeps your Lambda->NodePort and node<->node working)
  ingress {
    description = "intra-sg"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # Optional SSH from a specific CIDR
  dynamic "ingress" {
    for_each = var.ssh_ingress_cidr != "" ? [var.ssh_ingress_cidr] : []
    content {
      description = "ssh"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}



locals {
  # pick 2 subnets for Lambda VPC config
  lambda_subnets = slice(data.aws_subnets.default.ids, 0, 2)

  # ----------------------------
  # Lambda code: API producer
  # ----------------------------
  api_lambda_py = <<-PY
import json
import os
import urllib.request

BRIDGE_BASE = os.environ["BRIDGE_BASE"].rstrip("/")  # e.g. http://10.x.x.x:30082

def _post(topic: str, payload: dict):
    url = f"{BRIDGE_BASE}/topics/{topic}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("utf-8")

def handler(event, context):
    # HTTP API v2 event
    path = event.get("rawPath","")
    body = event.get("body") or "{}"
    try:
        payload = json.loads(body)
    except Exception:
        return {"statusCode": 400, "body": "Invalid JSON"}

    if path.startswith("/products"):
        topic = "products"
    elif path.startswith("/orders"):
        topic = "orders"
    elif path.startswith("/suppliers"):
        topic = "suppliers"
    else:
        return {"statusCode": 404, "body": "Unknown route"}

    _post(topic, payload)
    return {"statusCode": 200, "body": "OK"}
PY

  # ----------------------------
  # Lambda code: S3 producer
  # (expects uploaded file to be a JSON array)
  # ----------------------------
  s3_lambda_py = <<-PY
import json
import os
import urllib.request
import boto3

s3 = boto3.client("s3")
BRIDGE_BASE = os.environ["BRIDGE_BASE"].rstrip("/")

def _post(topic: str, payload: dict):
    url = f"{BRIDGE_BASE}/topics/{topic}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("utf-8")

def handler(event, context):
    # S3 put event
    rec = event["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key = rec["s3"]["object"]["key"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read().decode("utf-8")
    data = json.loads(raw)

    # Decide topic by filename
    lower = key.lower()
    if "products" in lower:
        topic = "products"
    elif "orders" in lower:
        topic = "orders"
    elif "suppliers" in lower:
        topic = "suppliers"
    else:
        # ignore unknown files
        return {"statusCode": 200, "body": "Ignored"}

    # publish each record
    if isinstance(data, list):
        for item in data:
            _post(topic, item)
    else:
        _post(topic, data)

    return {"statusCode": 200, "body": "OK"}
PY

  # ----------------------------
  # Kubernetes manifest (applied on master)
  # - Kafka (Helm)
  # - Elasticsearch + Kibana (simple single-node)
  # - Fluent Bit to ES
  # - REST Bridge (FastAPI-ish minimal HTTP->Kafka producer)
  # - 3 consumers (products/orders/suppliers) -> ES indices
  #
  # NOTE: For speed/reliability in a course project, containers pip-install at startup.
  # ----------------------------
  platform_yaml = <<-YAML
apiVersion: v1
kind: Namespace
metadata:
  name: platform
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-code
  namespace: platform
data:
  bridge.py: |
    import json, os
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from confluent_kafka import Producer

    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka.platform.svc.cluster.local:9092")

    p = Producer({"bootstrap.servers": BOOTSTRAP})

    class H(BaseHTTPRequestHandler):
      def do_POST(self):
        # Expect: POST /topics/<topic> with JSON body
        parts = self.path.strip("/").split("/")
        if len(parts) != 2 or parts[0] != "topics":
          self.send_response(404); self.end_headers(); return
        topic = parts[1]
        length = int(self.headers.get("Content-Length","0"))
        body = self.rfile.read(length).decode("utf-8") if length>0 else "{}"
        try:
          payload = json.loads(body)
        except:
          self.send_response(400); self.end_headers(); self.wfile.write(b"bad json"); return
        p.produce(topic, json.dumps(payload).encode("utf-8"))
        p.flush(5)
        self.send_response(200); self.end_headers(); self.wfile.write(b"OK")

    if __name__ == "__main__":
      HTTPServer(("0.0.0.0",8080), H).serve_forever()

  consumer.py: |
    import json, os, time, urllib.request
    from confluent_kafka import Consumer

    TOPIC = os.environ["TOPIC"]
    GROUP = os.environ.get("GROUP", f"{TOPIC}-cg")
    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka.platform.svc.cluster.local:9092")
    ES = os.getenv("ES_URL","http://elasticsearch.platform.svc.cluster.local:9200")
    INDEX = os.environ["INDEX"]

    def http(method, url, body=None, headers=None):
      headers = headers or {}
      data = None
      if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
      req = urllib.request.Request(url, data=data, headers=headers, method=method)
      with urllib.request.urlopen(req, timeout=10) as r:
        return r.read().decode("utf-8")

    def ensure_index():
      # create index with a basic mapping if missing
      try:
        http("HEAD", f"{ES}/{INDEX}")
      except:
        # very lightweight mapping: date fields as date if present
        mapping = {
          "mappings": {
            "dynamic": True,
            "properties": {
              "date": {"type":"date"},
              "price": {"type":"double"},
              "total_amount": {"type":"double"},
              "stock_quantity": {"type":"integer"},
              "id": {"type":"integer"},
              "supplier_id": {"type":"integer"}
            }
          }
        }
        http("PUT", f"{ES}/{INDEX}", mapping)

    def index_doc(doc):
      # Use id if exists, else let ES generate
      doc_id = doc.get("id")
      if doc_id is not None:
        http("PUT", f"{ES}/{INDEX}/_doc/{doc_id}", doc)
      else:
        http("POST", f"{ES}/{INDEX}/_doc", doc)

    if __name__ == "__main__":
      ensure_index()
      c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
      })
      c.subscribe([TOPIC])
      print(f"[consumer] topic={TOPIC} index={INDEX} bootstrap={BOOTSTRAP} es={ES}")
      while True:
        msg = c.poll(1.0)
        if msg is None:
          continue
        if msg.error():
          print("[consumer] error:", msg.error())
          continue
        try:
          doc = json.loads(msg.value().decode("utf-8"))
          index_doc(doc)
          print("[consumer] indexed", TOPIC, doc.get("id"))
        except Exception as e:
          print("[consumer] bad message:", e)

---
# Elasticsearch (single-node, no security for course project)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: elasticsearch
  namespace: platform
spec:
  replicas: 1
  selector:
    matchLabels: { app: elasticsearch }
  template:
    metadata:
      labels: { app: elasticsearch }
    spec:
      containers:
        - name: es
          image: docker.elastic.co/elasticsearch/elasticsearch:8.11.3
          env:
            - name: discovery.type
              value: single-node
            - name: xpack.security.enabled
              value: "false"
            - name: ES_JAVA_OPTS
              value: "-Xms1g -Xmx1g"
          ports:
            - containerPort: 9200
---
apiVersion: v1
kind: Service
metadata:
  name: elasticsearch
  namespace: platform
spec:
  selector: { app: elasticsearch }
  ports:
    - name: http
      port: 9200
      targetPort: 9200
  type: ClusterIP
---
# Kibana
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kibana
  namespace: platform
spec:
  replicas: 1
  selector:
    matchLabels: { app: kibana }
  template:
    metadata:
      labels: { app: kibana }
    spec:
      containers:
        - name: kibana
          image: docker.elastic.co/kibana/kibana:8.11.3
          env:
            - name: ELASTICSEARCH_HOSTS
              value: http://elasticsearch.platform.svc.cluster.local:9200
          ports:
            - containerPort: 5601
---
apiVersion: v1
kind: Service
metadata:
  name: kibana
  namespace: platform
spec:
  selector: { app: kibana }
  ports:
    - port: 5601
      targetPort: 5601
      nodePort: 30601
  type: NodePort
---
# REST Bridge (HTTP -> Kafka) on NodePort 30082
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-rest-bridge
  namespace: platform
spec:
  replicas: 1
  selector:
    matchLabels: { app: kafka-rest-bridge }
  template:
    metadata:
      labels: { app: kafka-rest-bridge }
    spec:
      containers:
        - name: bridge
          image: python:3.11-slim
          env:
            - name: KAFKA_BOOTSTRAP
              value: kafka.platform.svc.cluster.local:9092
          command: ["sh","-c"]
          args:
            - pip install --no-cache-dir confluent-kafka==2.5.0 && python /code/bridge.py
          volumeMounts:
            - name: code
              mountPath: /code
      volumes:
        - name: code
          configMap:
            name: app-code
            items:
              - key: bridge.py
                path: bridge.py
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-rest-bridge
  namespace: platform
spec:
  selector: { app: kafka-rest-bridge }
  ports:
    - port: 8080
      targetPort: 8080
      nodePort: 30082
  type: NodePort
---
# Consumers
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer-products
  namespace: platform
spec:
  replicas: 1
  selector:
    matchLabels: { app: consumer-products }
  template:
    metadata:
      labels: { app: consumer-products }
    spec:
      containers:
        - name: c
          image: python:3.11-slim
          env:
            - name: TOPIC
              value: products
            - name: INDEX
              value: products-index
            - name: ES_URL
              value: http://elasticsearch.platform.svc.cluster.local:9200
            - name: KAFKA_BOOTSTRAP
              value: kafka.platform.svc.cluster.local:9092
          command: ["sh","-c"]
          args:
            - pip install --no-cache-dir confluent-kafka==2.5.0 && python /code/consumer.py
          volumeMounts:
            - name: code
              mountPath: /code
      volumes:
        - name: code
          configMap:
            name: app-code
            items:
              - key: consumer.py
                path: consumer.py
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer-orders
  namespace: platform
spec:
  replicas: 1
  selector:
    matchLabels: { app: consumer-orders }
  template:
    metadata:
      labels: { app: consumer-orders }
    spec:
      containers:
        - name: c
          image: python:3.11-slim
          env:
            - name: TOPIC
              value: orders
            - name: INDEX
              value: orders-index
            - name: ES_URL
              value: http://elasticsearch.platform.svc.cluster.local:9200
            - name: KAFKA_BOOTSTRAP
              value: kafka.platform.svc.cluster.local:9092
          command: ["sh","-c"]
          args:
            - pip install --no-cache-dir confluent-kafka==2.5.0 && python /code/consumer.py
          volumeMounts:
            - name: code
              mountPath: /code
      volumes:
        - name: code
          configMap:
            name: app-code
            items:
              - key: consumer.py
                path: consumer.py
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer-suppliers
  namespace: platform
spec:
  replicas: 1
  selector:
    matchLabels: { app: consumer-suppliers }
  template:
    metadata:
      labels: { app: consumer-suppliers }
    spec:
      containers:
        - name: c
          image: python:3.11-slim
          env:
            - name: TOPIC
              value: suppliers
            - name: INDEX
              value: suppliers-index
            - name: ES_URL
              value: http://elasticsearch.platform.svc.cluster.local:9200
            - name: KAFKA_BOOTSTRAP
              value: kafka.platform.svc.cluster.local:9092
          command: ["sh","-c"]
          args:
            - pip install --no-cache-dir confluent-kafka==2.5.0 && python /code/consumer.py
          volumeMounts:
            - name: code
              mountPath: /code
      volumes:
        - name: code
          configMap:
            name: app-code
            items:
              - key: consumer.py
                path: consumer.py
YAML

  # Kafka Helm values (single-broker, no persistence for simplicity)
  kafka_values = <<-YAML
kraft:
  enabled: true
zookeeper:
  enabled: false
replicaCount: 1
persistence:
  enabled: false
listeners:
  client:
    protocol: PLAINTEXT
YAML

  # ----------------------------
  # Master bootstrap script:
  # installs k3s + helm, deploys Kafka + platform yaml
  # ----------------------------
  master_user_data = <<EOF
#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# basic tools
apt-get update -y
# Install & start SSM Agent (Ubuntu)
snap install amazon-ssm-agent --classic
systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service || true
systemctl restart snap.amazon-ssm-agent.amazon-ssm-agent.service || true
apt-get install -y curl unzip

# install k3s server
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --write-kubeconfig-mode 644" sh -

# wait for node ready
sleep 15

# install helm
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# kubeconfig
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

kubectl create namespace platform || true

# write Kafka values + platform manifest
cat >/tmp/kafka-values.yaml <<-KVAL
${local.kafka_values}
KVAL

cat >/tmp/platform.yaml <<-PYAML
${local.platform_yaml}
PYAML

helm repo add bitnami https://charts.bitnami.com/bitnami
helm upgrade --install kafka bitnami/kafka -n platform -f /tmp/kafka-values.yaml

# wait a bit for kafka service DNS
sleep 20

kubectl apply -f /tmp/platform.yaml

echo "BOOTSTRAP DONE"
EOF

  # Worker joins master
  worker_user_data = <<EOF
#!/bin/bash
# Install & start SSM Agent (Ubuntu)
snap install amazon-ssm-agent --classic
systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service || true
systemctl restart snap.amazon-ssm-agent.amazon-ssm-agent.service || true
set -euo pipefail
curl -sfL https://get.k3s.io | K3S_URL="https://${aws_instance.k3s_master.private_ip}:6443" K3S_TOKEN="${random_password.k3s_token.result}" sh -
EOF
}

# ----------------------------
# Random token for k3s join
# ----------------------------
resource "random_password" "k3s_token" {
  length  = 32
  special = false
}

# ----------------------------
# IAM for EC2 (SSM + S3 read not needed here, but SSM is very useful)
# ----------------------------
resource "aws_iam_role" "ec2_role" {
  name = "${var.project_name}-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Effect    = "Allow",
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.project_name}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# ----------------------------
# EC2 instances (k3s master + worker)
# ----------------------------
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "k3s_master" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = local.lambda_subnets[0]
  vpc_security_group_ids = [aws_security_group.project.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2_profile.name
  user_data                   = local.master_user_data
  associate_public_ip_address = true
  key_name="MyKey"
  tags = {
    Name = "${var.project_name}-k3s-master"
  }
}

resource "aws_instance" "k3s_worker" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = local.lambda_subnets[1]
  vpc_security_group_ids = [aws_security_group.project.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2_profile.name
  user_data                   = local.worker_user_data
  associate_public_ip_address = true
  key_name="MyKey"
  depends_on = [aws_instance.k3s_master]

  tags = {
    Name = "${var.project_name}-k3s-worker"
  }
}

# ----------------------------
# S3 bucket (uploads trigger lambda only under ingest/ prefix)
# ----------------------------
resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data" {
  bucket = "${var.project_name}-data-${random_id.suffix.hex}"
}

# ----------------------------
# IAM for Lambda
# ----------------------------
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3_read" {
  name = "${var.project_name}-lambda-s3-read"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:GetObject"],
      Resource = ["${aws_s3_bucket.data.arn}/*"]
    }]
  })
}

# ----------------------------
# Package Lambdas
# ----------------------------
data "archive_file" "api_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/api_lambda.zip"
  source {
    content  = local.api_lambda_py
    filename = "lambda_function.py"
  }
}

data "archive_file" "s3_lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/s3_lambda.zip"
  source {
    content  = local.s3_lambda_py
    filename = "lambda_function.py"
  }
}

# ----------------------------
# Lambda functions (in VPC so they can reach master private IP:NodePort)
# ----------------------------
resource "aws_lambda_function" "api_producer" {
  function_name = "${var.project_name}-api-producer"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.handler"
  runtime       = "python3.11"

  filename         = data.archive_file.api_lambda_zip.output_path
  source_code_hash = data.archive_file.api_lambda_zip.output_base64sha256

  vpc_config {
    subnet_ids         = local.lambda_subnets
    security_group_ids = [aws_security_group.project.id]
  }

  environment {
    variables = {
      BRIDGE_BASE = "http://${aws_instance.k3s_master.private_ip}:${var.bridge_nodeport}"
    }
  }

  depends_on = [aws_instance.k3s_master]
}

resource "aws_lambda_function" "s3_producer" {
  function_name = "${var.project_name}-s3-producer"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.handler"
  runtime       = "python3.11"

  filename         = data.archive_file.s3_lambda_zip.output_path
  source_code_hash = data.archive_file.s3_lambda_zip.output_base64sha256

  vpc_config {
    subnet_ids         = local.lambda_subnets
    security_group_ids = [aws_security_group.project.id]
  }

  environment {
    variables = {
      BRIDGE_BASE = "http://${aws_instance.k3s_master.private_ip}:${var.bridge_nodeport}"
    }
  }

  depends_on = [aws_instance.k3s_master]
}

# S3 notification (only for ingest/ prefix)
resource "aws_s3_bucket_notification" "notify" {
  bucket = aws_s3_bucket.data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_producer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.data_prefix
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_producer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data.arn
}

# ----------------------------
# API Gateway HTTP API -> Lambda routes
# ----------------------------
resource "aws_apigatewayv2_api" "http_api" {
  name          = "${var.project_name}-http-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "lambda_integ" {
  api_id                 = aws_apigatewayv2_api.http_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api_producer.arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "products" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "POST /products"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integ.id}"
}

resource "aws_apigatewayv2_route" "orders" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "POST /orders"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integ.id}"
}

resource "aws_apigatewayv2_route" "suppliers" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "POST /suppliers"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integ.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http_api.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "allow_apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_producer.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api.execution_arn}/*/*"
}
