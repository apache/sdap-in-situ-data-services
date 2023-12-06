//resource "aws_elasticsearch_domain" "ideas-es" {
//  domain_name    = "${var.prefix}-ideas-es"
////  engine_version = "Elasticsearch_7.10"
//// Not supported that in elastic_search
//  elasticsearch_version = "7.10"
//  cluster_config {
//    instance_count = var.ideas_es_cluster_instance_count
//    instance_type  = var.ideas_es_cluster_instance_type
//  }
//
////  advanced_security_options {
////    enabled                        = false
////    anonymous_auth_enabled         = true
////    internal_user_database_enabled = true
////    master_user_options {
////      master_user_name     = "example"
////      master_user_password = "Barbarbarbar1!"
////    }
////  }
//
//  vpc_options {
//    subnet_ids         = [var.ideas_api_lambda_subnet_ids[0]]
//    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
//  }
//  encrypt_at_rest {
//    enabled = true
//  }
//  node_to_node_encryption {
//    enabled = true
//  }
//  ebs_options {
//    ebs_enabled = true
//    volume_type = "gp2"
////    throughput = 125
//    volume_size = 10
//  }
//  access_policies = templatefile(
//    "${path.module}/es_access_policy.json",
//    {
//      es_resource: "arn:aws:es:${var.aws_region}:${var.account_id}:domain/${var.prefix}-es/*"
//    }
//  )
//  tags = {
////    Domain = "TestDomain"
//  }
//}

data "aws_elasticsearch_domain" "ideas-es" {
  domain_name = "ideas-api-dev-1"
}