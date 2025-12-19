output "k3s_master_instance_id" {
  value = aws_instance.k3s_master.id
}

output "k3s_master_private_ip" {
  value = aws_instance.k3s_master.private_ip
}

output "api_invoke_url" {
  value = aws_apigatewayv2_api.http_api.api_endpoint
}

output "data_bucket_name" {
  value = aws_s3_bucket.data.bucket
}

output "kibana_url_hint" {
  value = "Kibana NodePort (from inside VPC): http://${aws_instance.k3s_master.private_ip}:${var.kibana_nodeport}"
}

output "bridge_url_hint" {
  value = "Kafka REST Bridge (from inside VPC): http://${aws_instance.k3s_master.private_ip}:${var.bridge_nodeport}"
}
