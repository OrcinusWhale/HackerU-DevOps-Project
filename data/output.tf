output "k3s_master_public_ip" {
  value       = aws_instance.k3s_master.public_ip
  description = "Public IP of the k3s master node"
}

output "k3s_worker_public_ips" {
  # CHANGED: Added [*] to handle multiple worker nodes (since node_count is 2)
  value       = aws_instance.k3s_worker[*].public_ip
  description = "Public IPs of the k3s worker nodes"
}

output "k3s_master_private_ip" {
  value       = aws_instance.k3s_master.private_ip
  description = "Private IP of the k3s master node"
}

output "s3_bucket_name" {
  value       = aws_s3_bucket.data.bucket
  description = "S3 bucket that triggers the S3->Kafka producer Lambda"
}

output "http_api_endpoint" {
  value       = aws_apigatewayv2_api.http_api.api_endpoint
  description = "Base URL for the API Gateway HTTP API"
}

output "http_api_products_url" {
  value = "${aws_apigatewayv2_api.http_api.api_endpoint}/products"
}

output "http_api_orders_url" {
  value = "${aws_apigatewayv2_api.http_api.api_endpoint}/orders"
}

output "http_api_suppliers_url" {
  value = "${aws_apigatewayv2_api.http_api.api_endpoint}/suppliers"
}

output "bridge_url_public" {
  value       = "http://${aws_instance.k3s_master.public_ip}:${var.bridge_nodeport}"
  description = "Kafka REST bridge via master public IP (requires SG allowlist)"
}

output "kibana_url_public" {
  value       = "http://${aws_instance.k3s_master.public_ip}:${var.kibana_nodeport}"
  description = "Kibana via master public IP (requires deploy_platform=true)"
}

output "elasticsearch_url_public" {
  value       = "http://${aws_instance.k3s_master.public_ip}:${var.elasticsearch_nodeport}"
  description = "Elasticsearch via master public IP (requires deploy_platform=true)"
}
