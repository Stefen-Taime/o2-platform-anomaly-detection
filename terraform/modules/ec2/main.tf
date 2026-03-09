# =============================================================================
# Module: EC2 — MLflow + Jenkins + IAM + Key Pair
# =============================================================================

# Latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# --- SSH Key Pair ---
resource "aws_key_pair" "main" {
  key_name   = "${var.project}-key"
  public_key = file("${path.root}/o2-key.pub")

  tags = {
    Name = "${var.project}-key"
  }
}

# --- EC2 IAM Role ---
resource "aws_iam_role" "ec2_role" {
  name = "${var.project}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "ec2_s3" {
  name = "${var.project}-ec2-s3"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ]
      Resource = [
        var.artifacts_bucket_arn,
        "${var.artifacts_bucket_arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "ec2_eks" {
  name = "${var.project}-ec2-eks"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "eks:DescribeCluster",
        "eks:ListClusters"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.project}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# =============================================================================
# MLflow Server (t3.micro)
# =============================================================================

resource "aws_instance" "mlflow" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.mlflow_instance_type
  key_name               = aws_key_pair.main.key_name
  subnet_id              = var.public_subnet_ids[0]
  vpc_security_group_ids = [var.mlflow_sg_id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = base64encode(<<-EOF
#!/bin/bash
set -euo pipefail

exec > /var/log/mlflow-setup.log 2>&1

echo "=== MLflow Server Setup ==="

# Install Python 3.11 + pip
dnf install -y python3.11 python3.11-pip

# Install MLflow
python3.11 -m pip install mlflow==2.14.0 boto3

# Create MLflow directories
mkdir -p /opt/mlflow/{artifacts,backend}
chown -R ec2-user:ec2-user /opt/mlflow

# Create systemd service (no leading spaces!)
cat > /etc/systemd/system/mlflow.service <<'MLFLOW_SERVICE'
[Unit]
Description=MLflow Tracking Server
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/mlflow
ExecStart=/usr/local/bin/mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri sqlite:////opt/mlflow/backend/mlflow.db --default-artifact-root s3://${var.artifacts_bucket_id}/mlflow-artifacts
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
MLFLOW_SERVICE

systemctl daemon-reload
systemctl enable mlflow
systemctl start mlflow

echo "=== MLflow Server Ready ==="
  EOF
  )

  tags = {
    Name = "${var.project}-mlflow"
  }
}

# =============================================================================
# Jenkins Server (t3.small)
# =============================================================================

resource "aws_instance" "jenkins" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.jenkins_instance_type
  key_name               = aws_key_pair.main.key_name
  subnet_id              = var.public_subnet_ids[1]
  vpc_security_group_ids = [var.jenkins_sg_id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = base64encode(<<-EOF
#!/bin/bash
set -euo pipefail

exec > /var/log/jenkins-setup.log 2>&1

echo "=== Jenkins Server Setup ==="

# Install Java 17, git, docker, wget, unzip (wget/unzip not included in AL2023)
dnf install -y java-17-amazon-corretto git docker wget unzip

# Start Docker
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# Add Jenkins repo
wget -O /etc/yum.repos.d/jenkins.repo https://pkg.jenkins.io/redhat-stable/jenkins.repo
rpm --import https://pkg.jenkins.io/redhat-stable/jenkins.io-2023.key

# Install Jenkins
dnf install -y jenkins

# Configure Jenkins — skip setup wizard
mkdir -p /var/lib/jenkins/init.groovy.d

cat > /var/lib/jenkins/init.groovy.d/basic-security.groovy <<'GROOVY'
import jenkins.model.*
import hudson.security.*

def instance = Jenkins.getInstance()
def hudsonRealm = new HudsonPrivateSecurityRealm(false)
hudsonRealm.createAccount("admin", System.getenv("JENKINS_ADMIN_PASSWORD") ?: "changeme")
instance.setSecurityRealm(hudsonRealm)

def strategy = new FullControlOnceLoggedInAuthorizationStrategy()
strategy.setAllowAnonymousRead(false)
instance.setAuthorizationStrategy(strategy)
instance.save()
GROOVY

# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
cd /tmp && unzip -q awscliv2.zip && ./aws/install

# Install kubectl
curl -LO "https://dl.k8s.io/release/v1.29.0/bin/linux/amd64/kubectl"
install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Install pip + databricks CLI
dnf install -y python3.11 python3.11-pip
python3.11 -m pip install databricks-cli mlflow==2.14.0

# Set environment variables for Jenkins
cat > /etc/profile.d/jenkins-env.sh <<'ENVFILE'
export MLFLOW_TRACKING_URI=http://${aws_instance.mlflow.private_ip}:5000
export ARTIFACTS_BUCKET=${var.artifacts_bucket_id}
export AWS_DEFAULT_REGION=${var.region}
ENVFILE

# Jenkins env vars
echo 'JAVA_ARGS="-Djenkins.install.runSetupWizard=false"' >> /etc/sysconfig/jenkins

# Start Jenkins
systemctl enable jenkins
systemctl start jenkins

echo "=== Jenkins Server Ready ==="
echo "URL: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):8080"
echo "User: admin / Password: <configured in Groovy init script>"
  EOF
  )

  depends_on = [aws_instance.mlflow]

  tags = {
    Name = "${var.project}-jenkins"
  }
}
