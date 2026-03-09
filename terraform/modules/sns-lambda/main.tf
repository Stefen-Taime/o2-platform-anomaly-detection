# =============================================================================
# Module: SNS + Lambda — Real-time Slack Alerting (PRD §4.2)
# =============================================================================

# SNS Topic for anomaly alerts
resource "aws_sns_topic" "anomaly_alerts" {
  name = "${var.project}-anomaly-alerts"

  tags = {
    Name = "${var.project}-anomaly-alerts"
  }
}

# Lambda execution role
resource "aws_iam_role" "lambda_role" {
  name = "${var.project}-lambda-slack-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_role.name
}

# Lambda function — Slack notifier
resource "aws_lambda_function" "slack_notifier" {
  function_name = "${var.project}-slack-notifier"
  role          = aws_iam_role.lambda_role.arn
  handler       = "index.handler"
  runtime       = "python3.12"
  timeout       = 10
  memory_size   = 128

  filename         = "${path.root}/lambda/slack_notifier.zip"
  source_code_hash = filebase64sha256("${path.root}/lambda/slack_notifier.zip")

  environment {
    variables = {
      SLACK_WEBHOOK_URL = var.slack_webhook_url
    }
  }

  tags = {
    Name = "${var.project}-slack-notifier"
  }
}

# SNS → Lambda subscription
resource "aws_sns_topic_subscription" "lambda_sub" {
  topic_arn = aws_sns_topic.anomaly_alerts.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.slack_notifier.arn
}

# Allow SNS to invoke Lambda
resource "aws_lambda_permission" "sns_invoke" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.slack_notifier.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.anomaly_alerts.arn
}
