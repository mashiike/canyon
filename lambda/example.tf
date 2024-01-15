
resource "aws_iam_role" "canyon_example" {
  name = "canyon-example"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "scheduler.amazonaws.com",
          ]
        }
      }
    ]
  })
}

resource "aws_iam_policy" "canyon_example" {
  name   = "canyon-example"
  path   = "/"
  policy = data.aws_iam_policy_document.canyon_example.json
}

resource "aws_iam_role_policy_attachment" "canyon_example" {
  role       = aws_iam_role.canyon_example.name
  policy_arn = aws_iam_policy.canyon_example.arn
}

data "aws_iam_policy_document" "canyon_example" {
  statement {
    resources = [
      aws_sqs_queue.canyon_example.arn,
      aws_sqs_queue.canyon_websocket_example.arn,
    ]
    actions = [
      "sqs:DeleteMessage",
      "sqs:GetQueueUrl",
      "sqs:ChangeMessageVisibility",
      "sqs:ReceiveMessage",
      "sqs:SendMessage",
      "sqs:GetQueueAttributes",
    ]
  }
  statement {
    resources = [
      local.canyon_temporary_bucket_arn,
      "${local.canyon_temporary_bucket_arn}/*"
    ]
    actions = [
      "s3:PutObject",
      "s3:GetObject",
    ]
  }
  statement {
    resources = ["*"]
    actions = [
      "lambda:ListEventSourceMappings",
      "execute-api:ManageConnections",
    ]
  }
  statement {
    actions = [
      "logs:GetLog*",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }
  // for use schedler
  statement {
    actions = [
      "scheduler:CreateSchedule",
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "iam:PassRole",
    ]
    resources = [
      aws_iam_role.canyon_example.arn,
    ]
  }
}

## for simple example
resource "aws_sqs_queue" "canyon_example" {
  name                       = "canyon-example"
  message_retention_seconds  = 86400
  visibility_timeout_seconds = 300
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.canyon_example_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue" "canyon_example_dlq" {
  name                      = "canyon-example-dlq"
  message_retention_seconds = 345600
}

data "archive_file" "canyon_example_dummy" {
  type        = "zip"
  output_path = "${path.module}/canyon_example_dummy.zip"
  source {
    content  = "canyon_example_dummy"
    filename = "bootstrap"
  }
  depends_on = [
    null_resource.canyon_example_dummy
  ]
}

resource "null_resource" "canyon_example_dummy" {}

resource "aws_lambda_function" "canyon_example" {
  lifecycle {
    ignore_changes = all
  }

  function_name = "canyon-example"
  role          = aws_iam_role.canyon_example.arn

  handler  = "bootstrap"
  runtime  = "provided.al2"
  filename = data.archive_file.canyon_example_dummy.output_path
}

resource "aws_lambda_alias" "canyon_example" {
  lifecycle {
    ignore_changes = all
  }
  name             = "current"
  function_name    = aws_lambda_function.canyon_example.arn
  function_version = aws_lambda_function.canyon_example.version
}


resource "aws_lambda_function_url" "canyon_example" {
  function_name      = aws_lambda_alias.canyon_example.function_name
  qualifier          = aws_lambda_alias.canyon_example.name
  authorization_type = "NONE"

  cors {
    allow_credentials = true
    allow_origins     = ["*"]
    allow_methods     = ["GET", "POST"]
    expose_headers    = ["keep-alive", "date"]
    max_age           = 0
  }
}

resource "aws_lambda_event_source_mapping" "canyon_example" {
  batch_size                         = 10
  event_source_arn                   = aws_sqs_queue.canyon_example.arn
  enabled                            = true
  maximum_batching_window_in_seconds = 5
  function_name                      = aws_lambda_alias.canyon_example.arn
  function_response_types            = ["ReportBatchItemFailures"]
}

output "lambda_function_url" {
  description = "Generated function URL"
  value       = aws_lambda_function_url.canyon_example.function_url
}

## for websocket example
resource "aws_sqs_queue" "canyon_websocket_example" {
  name                       = "canyon-websocket-example"
  message_retention_seconds  = 86400
  visibility_timeout_seconds = 300
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.canyon_websocket_example_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue" "canyon_websocket_example_dlq" {
  name                      = "canyon-websocket-example-dlq"
  message_retention_seconds = 345600
}

data "archive_file" "canyon_websocket_example_dummy" {
  type        = "zip"
  output_path = "${path.module}/canyon_websocket_example_dummy.zip"
  source {
    content  = "canyon_websocket_example_dummy"
    filename = "bootstrap"
  }
  depends_on = [
    null_resource.canyon_example_dummy
  ]
}

resource "null_resource" "canyon_websocket_example_dummy" {}

resource "aws_lambda_function" "canyon_websocket_example" {
  lifecycle {
    ignore_changes = all
  }

  function_name = "canyon-websocket-example"
  role          = aws_iam_role.canyon_example.arn

  handler  = "bootstrap"
  runtime  = "provided.al2"
  filename = data.archive_file.canyon_websocket_example_dummy.output_path
}

resource "aws_lambda_alias" "canyon_websocket_example" {
  lifecycle {
    ignore_changes = all
  }
  name             = "current"
  function_name    = aws_lambda_function.canyon_websocket_example.arn
  function_version = aws_lambda_function.canyon_websocket_example.version
}


resource "aws_lambda_function_url" "canyon_websocket_example" {
  function_name      = aws_lambda_alias.canyon_websocket_example.function_name
  qualifier          = aws_lambda_alias.canyon_websocket_example.name
  authorization_type = "NONE"

  cors {
    allow_credentials = true
    allow_origins     = ["*"]
    allow_methods     = ["GET"]
    expose_headers    = ["keep-alive", "date"]
    max_age           = 0
  }
}

resource "aws_lambda_event_source_mapping" "canyon_websocket_example" {
  batch_size                         = 10
  event_source_arn                   = aws_sqs_queue.canyon_websocket_example.arn
  enabled                            = true
  maximum_batching_window_in_seconds = 5
  function_name                      = aws_lambda_alias.canyon_websocket_example.arn
  function_response_types            = ["ReportBatchItemFailures"]
}

output "lambda_function_url_for_websocket" {
  description = "Generated function URL for websocket example"
  value       = aws_lambda_function_url.canyon_websocket_example.function_url
}

resource "aws_apigatewayv2_api" "canyon_websocket_example" {
  name                       = "canyon-websocket-example-api"
  protocol_type              = "WEBSOCKET"
  route_selection_expression = "$request.body.action"
}

resource "aws_apigatewayv2_route" "canyon_websocket_connect" {
  api_id                              = aws_apigatewayv2_api.canyon_websocket_example.id
  route_key                           = "$connect"
  route_response_selection_expression = "$default"
  target                              = "integrations/${aws_apigatewayv2_integration.canyon_websocketl_connect.id}"
}

resource "aws_apigatewayv2_route_response" "canyon_websocket_connect" {
  api_id             = aws_apigatewayv2_api.canyon_websocket_example.id
  route_id           = aws_apigatewayv2_route.canyon_websocket_connect.id
  route_response_key = "$default"
}

resource "aws_apigatewayv2_integration" "canyon_websocketl_connect" {
  api_id           = aws_apigatewayv2_api.canyon_websocket_example.id
  integration_type = "AWS_PROXY"

  connection_type           = "INTERNET"
  content_handling_strategy = "CONVERT_TO_TEXT"
  integration_method        = "POST"
  integration_uri           = aws_lambda_alias.canyon_websocket_example.invoke_arn
  passthrough_behavior      = "WHEN_NO_MATCH"
}

resource "aws_apigatewayv2_route" "canyon_websocket_disconnect" {
  api_id                              = aws_apigatewayv2_api.canyon_websocket_example.id
  route_key                           = "$disconnect"
  route_response_selection_expression = "$default"
  target                              = "integrations/${aws_apigatewayv2_integration.canyon_websocket_disconnect.id}"
}

resource "aws_apigatewayv2_route_response" "canyon_websocket_disconnect" {
  api_id             = aws_apigatewayv2_api.canyon_websocket_example.id
  route_id           = aws_apigatewayv2_route.canyon_websocket_disconnect.id
  route_response_key = "$default"
}

resource "aws_apigatewayv2_integration" "canyon_websocket_disconnect" {
  api_id           = aws_apigatewayv2_api.canyon_websocket_example.id
  integration_type = "AWS_PROXY"

  connection_type           = "INTERNET"
  content_handling_strategy = "CONVERT_TO_TEXT"
  integration_method        = "POST"
  integration_uri           = aws_lambda_alias.canyon_websocket_example.invoke_arn
  passthrough_behavior      = "WHEN_NO_MATCH"
}

resource "aws_apigatewayv2_route" "canyon_websocket_default" {
  api_id                              = aws_apigatewayv2_api.canyon_websocket_example.id
  route_key                           = "$default"
  route_response_selection_expression = "$default"
  target                              = "integrations/${aws_apigatewayv2_integration.canyon_websocket_default.id}"
}

resource "aws_apigatewayv2_route_response" "canyon_websocket_default" {
  api_id             = aws_apigatewayv2_api.canyon_websocket_example.id
  route_id           = aws_apigatewayv2_route.canyon_websocket_default.id
  route_response_key = "$default"
}

resource "aws_apigatewayv2_integration" "canyon_websocket_default" {
  api_id           = aws_apigatewayv2_api.canyon_websocket_example.id
  integration_type = "AWS_PROXY"

  connection_type           = "INTERNET"
  content_handling_strategy = "CONVERT_TO_TEXT"
  integration_method        = "POST"
  integration_uri           = aws_lambda_alias.canyon_websocket_example.invoke_arn
  passthrough_behavior      = "WHEN_NO_MATCH"
}

resource "aws_apigatewayv2_stage" "canyon_websocket_example" {
  api_id      = aws_apigatewayv2_api.canyon_websocket_example.id
  name        = "prod"
  auto_deploy = true
}
