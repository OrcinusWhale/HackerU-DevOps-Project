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

resource "aws_security_group" "project" {
  name   = "${var.project_name}-sg"
  vpc_id = data.aws_vpc.default.id

  # SSH from your PC
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  # NodePorts
  ingress {
    description = "Kafka REST Bridge NodePort"
    from_port   = var.bridge_nodeport
    to_port     = var.bridge_nodeport
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  ingress {
    description = "Kibana NodePort"
    from_port   = var.kibana_nodeport
    to_port     = var.kibana_nodeport
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  ingress {
    description = "Elasticsearch NodePort"
    from_port   = var.elasticsearch_nodeport
    to_port     = var.elasticsearch_nodeport
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  # Intra-SG traffic
  ingress {
    description = "intra-sg"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

locals {
  lambda_subnets = slice(data.aws_subnets.default.ids, 0, 2)

  # ----------------------------
  # Lambda code: API producer
  # ----------------------------
  api_lambda_py = <<-PY
import json
import os
import urllib.request

BRIDGE_BASE = os.environ["BRIDGE_BASE"].rstrip("/")

def _post(partition: str, payload: dict):
    # Set timeout to 5s so Lambda doesn't hang if EC2 is down
    url = f"{BRIDGE_BASE}/partition/{partition}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"Connection Error: {e}")
        raise e

def handler(event, context):
    path = event.get("rawPath","")
    body = event.get("body") or "{}"
    try:
        payload = json.loads(body)
    except Exception:
        return {"statusCode": 400, "body": "Invalid JSON"}

    if path.startswith("/products"):
        partition = "0"
    elif path.startswith("/orders"):
        partition = "1"
    elif path.startswith("/suppliers"):
        partition = "2"
    else:
        return {"statusCode": 404, "body": "Unknown route"}

    try:
        _post(partition, payload)
        return {"statusCode": 200, "body": "OK"}
    except Exception as e:
        return {"statusCode": 502, "body": f"Bridge Error: {str(e)}"}
PY

  # ----------------------------
  # Lambda code: S3 producer
  # ----------------------------
  s3_lambda_py = <<-PY
# ...existing code...
import json
import os
import urllib.request
import boto3

s3 = boto3.client("s3")
BRIDGE_BASE = os.environ["BRIDGE_BASE"].rstrip("/")

def _post(partition: str, payload: dict):
    url = f"{BRIDGE_BASE}/partition/{partition}"
    print(f"Sending payload to {url}...", flush=True)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"POST failed to {url}: {e}", flush=True)
        raise e

def handler(event, context):
    try:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]

        print(f"Processing S3 object: {bucket}/{key}", flush=True)

        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read().decode("utf-8")
        data = json.loads(raw)

        lower = key.lower()
        if "products" in lower:
            partition = "0"
        elif "orders" in lower:
            partition = "1"
        elif "suppliers" in lower:
            partition = "2"
        else:
            print(f"Skipping key {key}: no matching topic found", flush=True)
            return {"statusCode": 200, "body": "Ignored"}

        if isinstance(data, list):
            print(f"Posting {len(data)} items to partition '{partition}'", flush=True)
            for item in data:
                _post(partition, item)
        else:
            print(f"Posting single item to partition '{partition}'", flush=True)
            _post(partition, data)

        return {"statusCode": 200, "body": "OK"}
    except Exception as e:
        print(f"Error processing S3 file: {str(e)}", flush=True)
        # We return 200 to stop S3 from retrying infinitely on bad data
        return {"statusCode": 200, "body": "Error Handled"}
PY

  # ----------------------------
  # Platform YAML (Kubernetes Manifests)
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
    import json, os, sys
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from confluent_kafka import Producer

    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","${aws_instance.kafka.private_ip}:9092")
    
    print(BOOTSTRAP)
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "message.timeout.ms": 3000,   # 3 Seconds max
        "delivery.timeout.ms": 3000,
        "queue.buffering.max.ms": 100 # Batch quickly
    }
    p = Producer(conf)

    class H(BaseHTTPRequestHandler):
      def do_POST(self):
        parts = self.path.strip("/").split("/")
        if len(parts) != 2 or parts[0] != "partition" or parts[1] not in ["0", "1", "2"]:
          self.send_response(404); self.end_headers(); return

        partition = int(parts[1])
        length = int(self.headers.get("Content-Length","0"))
        body = self.rfile.read(length).decode("utf-8") if length>0 else "{}"

        self.kafka_error = None
        def delivery_report(err, msg):
            if err is not None:
                self.kafka_error = err

        try:
          payload = json.loads(body)
          if isinstance(payload, list):
            for item in payload:
              p.produce("ecommerce", json.dumps(item).encode("utf-8"), callback=delivery_report, partition=partition)
              p.poll(0)
          else:
            p.produce("ecommerce", json.dumps(payload).encode("utf-8"), callback=delivery_report, partition=partition)
          
          # FIX: Flush shorter than Lambda timeout
          remaining = p.flush(3.0) 
          
          if remaining > 0:
             raise Exception("Kafka Timeout (Buffer not cleared)")

          if self.kafka_error:
             raise Exception(f"Kafka Delivery Failed: {self.kafka_error}")

          self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
          
        except Exception as e:
          print(f"ERROR: {e}", flush=True)
          self.send_response(500); self.end_headers(); self.wfile.write(str(e).encode("utf-8"))

    if __name__ == "__main__":
      HTTPServer(("0.0.0.0",8080), H).serve_forever()

  consumer.py: |
    import json
    import os
    import time
    import urllib.request
    import urllib.error
    from confluent_kafka import Consumer

    TOPIC = "ecommerce"

    GROUP = f"{TOPIC}-cg"
    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","${aws_instance.kafka.private_ip}:9092")
    ES = os.getenv("ES_URL","http://elasticsearch.platform.svc.cluster.local:9200").rstrip("/")

    BULK_DOCS = int(os.getenv("BULK_DOCS", "500"))
    FLUSH_SECS = float(os.getenv("FLUSH_SECS", "3.0"))

    def wait_for_kafka():
      while True:
        try:
          c0 = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"{GROUP}-bootstrap-check",
            "enable.auto.commit": False,
            "socket.timeout.ms": 3000,
            "session.timeout.ms": 6000,
          })
          c0.list_topics(timeout=5)
          c0.close()
          return
        except Exception as e:
          print(f"[consumer] waiting for kafka at {BOOTSTRAP}: {e}", flush=True)
          time.sleep(3)

    def http(method, url, body_bytes=None, headers=None, timeout=60):
      req = urllib.request.Request(url, data=body_bytes, headers=headers or {}, method=method)
      try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
          return r.status, r.read()
      except urllib.error.HTTPError as e:
        return e.code, e.read()
      except Exception as e:
        return 0, str(e).encode("utf-8","ignore")

    def ensure_index(index):
      st, _ = http("HEAD", f"{ES}/{index}", timeout=5)
      if st == 200:
        return

      mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
          "dynamic": True
        }
      }

      st, data = http(
        "PUT",
        f"{ES}/{index}",
        body_bytes=json.dumps(mapping).encode("utf-8"),
        headers={"Content-Type":"application/json"},
        timeout=30
      )
      if st not in (200, 201):
        print(f"[consumer] create index {index} failed:", st, data[:300].decode("utf-8","ignore"), flush=True)

    def bulk_index(pairs, index):
      lines = []
      for doc_id, doc in pairs:
        lines.append(json.dumps({"index": {"_id": doc_id}}))
        lines.append(json.dumps(doc))
      body = ("\n".join(lines) + "\n").encode("utf-8")

      st, data = http(
        "POST",
        f"{ES}/{index}/_bulk?refresh=false",
        body_bytes=body,
        headers={"Content-Type":"application/x-ndjson"},
        timeout=60
      )
      if st not in (200, 201):
        raise RuntimeError(f"bulk http status={st} body={data[:300].decode('utf-8','ignore')}")

      rj = json.loads(data.decode("utf-8","ignore") or "{}")
      if rj.get("errors"):
        for item in rj.get("items", []):
          i = item.get("index", {})
          if "error" in i:
             # Just log error, don't crash, so we process the valid ones
             print(f"[consumer] item error in {index}: {i['error']}", flush=True)

    if __name__ == "__main__":
      # Wait for ES to be up
      print(f"[consumer] waiting for ES at {ES}...", flush=True)
      while True:
        st, _ = http("GET", ES, timeout=5)
        if st == 200:
          break
        time.sleep(5)
      ensure_index("products")
      ensure_index("orders")
      ensure_index("suppliers")
      print(f"[consumer] waiting for Kafka at {BOOTSTRAP}...", flush=True)
      wait_for_kafka()

      c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
      })
      c.subscribe([TOPIC])

      print(f"[consumer] topic=ecommerce bulk_docs={BULK_DOCS} flush_secs={FLUSH_SECS}", flush=True)

      buf = [[], [], []]
      indexes = ("products", "orders", "suppliers")
      last_flush = [time.time(), time.time(), time.time()]
      last_msg = None
      list_counter = 0

      while True:
        msg = c.poll(1.0)
        now = time.time()

        if msg is not None:
          if msg.error():
            print("[consumer] error:", msg.error(), flush=True)
          else:
            try:
              print(f"[consumer] received message offset={msg.offset()}", flush=True)
              partition = msg.partition()
              payload = json.loads(msg.value().decode("utf-8"))
              if isinstance(payload, list):
                for item in payload:
                  doc_id = str(item.get("id") or f"{msg.topic()}-{partition}-{msg.offset()}-{list_counter}")
                  list_counter += 1
                  buf[partition].append((doc_id, item))
              else:
                doc_id = str(payload.get("id") or f"{msg.topic()}-{partition}-{msg.offset()}")
                buf[partition].append((doc_id, payload))
              last_msg = msg
            except Exception as e:
              print("[consumer] bad message:", e, flush=True)
        for i in range(3):
          if buf[i] and (len(buf[i]) >= BULK_DOCS or (now - last_flush[i]) >= FLUSH_SECS):
            try:
              bulk_index(buf[i], indexes[i])
              print(f"[consumer] bulk indexed {len(buf[i])} docs into {indexes[i]}", flush=True)
              if last_msg is not None:
                c.commit(message=last_msg, asynchronous=False)
            except Exception as e:
              print("[consumer] bulk failed (will retry):", e, flush=True)
            finally:
              buf[i] = []
              last_flush[i] = now
              list_counter = 0
---
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
      initContainers:
      - name: fix-permissions
        image: busybox
        command: ["sh", "-c", "chown -R 1000:1000 /usr/share/elasticsearch/data"]
        securityContext:
          runAsUser: 0
        volumeMounts:
        - name: es-data
          mountPath: /usr/share/elasticsearch/data
      containers:
        - name: es
          image: docker.elastic.co/elasticsearch/elasticsearch:8.11.3
          env:
            - name: discovery.type
              value: single-node
            - name: xpack.security.enabled
              value: "false"
            # FIX: Lower memory so it fits on t3.medium/large along with Kafka
            - name: ES_JAVA_OPTS
              value: "-Xms512m -Xmx512m"
          ports:
            - containerPort: 9200
          volumeMounts:
            - name: es-data
              mountPath: /usr/share/elasticsearch/data
      volumes:
        - name: es-data
          hostPath:
            path: /opt/elasticsearch-data
            type: DirectoryOrCreate
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
      nodePort: ${var.elasticsearch_nodeport}
  type: NodePort
---
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
      nodePort: ${var.kibana_nodeport}
  type: NodePort
---
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
            - name: PYTHONUNBUFFERED
              value: "1"
            - name: KAFKA_BOOTSTRAP
              value: ${aws_instance.kafka.private_ip}:9092
          command: ["sh","-c"]
          args:
            - pip install --no-cache-dir confluent-kafka==2.5.0 && python -u /code/bridge.py
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
      nodePort: ${var.bridge_nodeport}
  type: NodePort
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer
  namespace: platform
spec:
  replicas: 3
  selector:
    matchLabels: { app: consumer }
  template:
    metadata:
      labels: { app: consumer }
    spec:
      containers:
        - name: c
          image: python:3.11-slim
          env:
            - name: PYTHONUNBUFFERED
              value: "1"
            - name: ES_URL
              value: http://elasticsearch.platform.svc.cluster.local:9200
            - name: KAFKA_BOOTSTRAP
              value: ${aws_instance.kafka.private_ip}:9092
          command: ["sh","-c"]
          args:
            - pip install --no-cache-dir confluent-kafka==2.5.0 && python -u /code/consumer.py
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

  # ----------------------------
  # Master Bootstrap Script
  # ----------------------------
  master_user_data = <<EOF
#!/bin/bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y curl unzip ca-certificates awscli

# --- FIX START: ELASTICSEARCH MEMORY ---
# Set the limit immediately and make it persistent
sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" >> /etc/sysctl.conf
# --- FIX END ---

# --- FIX START: STORAGE PERMISSIONS ---
# Create a cron job that forces storage to be writable every minute.
# This ensures that whenever K3s creates a new volume folder, it gets fixed automatically.
mkdir -p /var/lib/rancher/k3s/storage
echo "* * * * * root chmod -R 777 /var/lib/rancher/k3s/storage >/dev/null 2>&1" > /etc/cron.d/fix-storage
chmod 0644 /etc/cron.d/fix-storage
# --- FIX END ---

# Avoid low ulimit issues on tiny instances (Kafka/ES)
ulimit -n 65536 || true

# ---- SSM agent ----
snap install amazon-ssm-agent --classic || true
systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service || true

# ---- k3s server ----
curl -sfL https://get.k3s.io | \
  K3S_TOKEN="${random_password.k3s_token.result}" \
  INSTALL_K3S_EXEC="server --write-kubeconfig-mode 644 --disable traefik --disable metrics-server" \
  sh -

until curl -k --max-time 2 -s https://127.0.0.1:6443/readyz >/dev/null; do
  echo "waiting for k3s apiserver..."
  sleep 3
done
echo "k3s apiserver is READY"

if [ "${var.deploy_platform}" = "true" ]; then
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  kubectl create namespace platform || true

  rm -f /tmp/platform.yaml || true
  aws s3 cp "s3://${aws_s3_bucket.data.bucket}/bootstrap/platform.yaml" /tmp/platform.yaml --region ${var.aws_region}

  echo "[BOOT] Applying platform.yaml..."
  kubectl apply -f /tmp/platform.yaml

  echo "[BOOT] Waiting for platform deployments..."
  kubectl -n platform rollout status deploy/elasticsearch --timeout=5m
  kubectl -n platform rollout status deploy/kibana --timeout=5m
  kubectl -n platform rollout status deploy/kafka-rest-bridge --timeout=5m
  kubectl -n platform rollout status deploy/consumer-products --timeout=5m
  kubectl -n platform rollout status deploy/consumer-orders --timeout=5m
  kubectl -n platform rollout status deploy/consumer-suppliers --timeout=5m

  echo "[BOOT] Platform services:"
  kubectl -n platform get svc
fi

echo "BOOTSTRAP DONE"
EOF

  worker_user_data = <<EOF
#!/bin/bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# --- FIX START: ELASTICSEARCH MEMORY (CRITICAL FOR WORKERS) ---
sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" >> /etc/sysctl.conf
# --- FIX END ---

# --- FIX START: STORAGE PERMISSIONS ---
# Forces the storage folder to be writable every minute.
mkdir -p /var/lib/rancher/k3s/storage
echo "* * * * * root chmod -R 777 /var/lib/rancher/k3s/storage >/dev/null 2>&1" > /etc/cron.d/fix-storage
chmod 0644 /etc/cron.d/fix-storage
# --- FIX END ---

snap install amazon-ssm-agent --classic || true
systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service || true

until curl -k --max-time 2 -s https://${aws_instance.k3s_master.private_ip}:6443/readyz >/dev/null; do
  echo "waiting for master..."
  sleep 3
done

curl -sfL https://get.k3s.io | \
  K3S_URL="https://${aws_instance.k3s_master.private_ip}:6443" \
  K3S_TOKEN="${random_password.k3s_token.result}" \
  sh -
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
# IAM for EC2
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

resource "aws_iam_role_policy" "ec2_s3_read" {
  name = "${var.project_name}-ec2-s3-read"
  role = aws_iam_role.ec2_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:GetObject"],
      Resource = ["${aws_s3_bucket.data.arn}/*"]
    }]
  })
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
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# ----------------------------
# EC2 instances (k3s master + worker)
# ----------------------------

resource "aws_instance" "k3s_master" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.master_instance_type
  subnet_id              = local.lambda_subnets[0]
  vpc_security_group_ids = [aws_security_group.project.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data                   = replace(local.master_user_data, "\r", "")
  user_data_replace_on_change = true

  associate_public_ip_address = true
  key_name                    = var.ssh_key_name != "" ? var.ssh_key_name : null

  depends_on = [aws_instance.kafka, aws_s3_object.platform_yaml]

  tags = { Name = "${var.project_name}-k3s-master" }
}

resource "aws_instance" "k3s_worker" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.worker_instance_type
  subnet_id              = local.lambda_subnets[1]
  vpc_security_group_ids = [aws_security_group.project.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data                   = replace(local.worker_user_data, "\r", "")
  user_data_replace_on_change = true

  associate_public_ip_address = true
  key_name                    = var.ssh_key_name != "" ? var.ssh_key_name : null

  depends_on = [aws_instance.k3s_master]

  tags = { Name = "${var.project_name}-k3s-worker" }
}

resource "aws_instance" "kafka" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.worker_instance_type
  vpc_security_group_ids = [aws_security_group.project.id]
  key_name               = var.ssh_key_name
  user_data              = <<-EOF
                            #!/bin/bash
                            apt-get update -y
                            apt-get install -y openjdk-17-jre
                            wget https://dlcdn.apache.org/kafka/4.1.1/kafka_2.13-4.1.1.tgz
                            tar -xzf kafka_2.13-4.1.1.tgz -C /opt/
                            cd /opt/kafka_2.13-4.1.1
                            PRIVATE_IP=$(hostname -I | awk '{print $1}')
                            echo "advertised.listeners=PLAINTEXT://$PRIVATE_IP:9092" >> config/server.properties
                            KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
                            bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
                            bin/kafka-server-start.sh -daemon config/server.properties
                            sleep 10
                            bin/kafka-topics.sh --create --topic ecommerce --partitions 3 --bootstrap-server localhost:9092
                            EOF
  tags                   = { Name = "kafka" }
}

# ----------------------------
# S3 bucket
# ----------------------------
resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data" {
  bucket = "${var.project_name}-data-${random_id.suffix.hex}"
}

resource "aws_s3_object" "platform_yaml" {
  bucket       = aws_s3_bucket.data.bucket
  key          = "bootstrap/platform.yaml"
  content      = local.platform_yaml
  content_type = "text/yaml"
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
# Lambda functions
# ----------------------------
resource "aws_lambda_function" "api_producer" {
  function_name = "${var.project_name}-api-producer"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.handler"
  runtime       = "python3.11"

  filename         = data.archive_file.api_lambda_zip.output_path
  source_code_hash = data.archive_file.api_lambda_zip.output_base64sha256
  timeout          = 300

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
  timeout          = 300

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

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"

  # CRITICAL: This adds the route to S3 in these specific route tables.
  # Use the route table ID(s) associated with your Lambda's private subnets.
  route_table_ids = [data.aws_vpc.default.main_route_table_id]

  tags = {
    Name = "s3-gateway-endpoint"
  }
}


resource "aws_s3_bucket_notification" "notify" {
  bucket = aws_s3_bucket.data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_producer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".json"
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
# API Gateway
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
