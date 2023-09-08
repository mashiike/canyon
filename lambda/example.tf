
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
          Service = "lambda.amazonaws.com"
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

resource "aws_sqs_queue" "canyon_example" {
  name                       = "canyon-example"
  message_retention_seconds  = 86400
  visibility_timeout_seconds = 30
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.canyon_example_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue" "canyon_example_dlq" {
  name                      = "canyon-example-dlq"
  message_retention_seconds = 345600
}

data "aws_iam_policy_document" "canyon_example" {
  statement {
    resources = [
      aws_sqs_queue.canyon_example.arn,
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
