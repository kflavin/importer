#!/bin/bash
set -vxe
sleep 1

#################################################################################################################
# Setup script for userdata.  This should be included in your lambda function prior to the userdata body.
#
# This will do the following:
#   1) find environment (region, instance id)
#   2) install the importer script
#   #) setup cloudwatch logging
#   4) register a cleanup handler that notifies our SNS topic on completion, and terminates the EC2 instance.
#
#
#
#  Lambda parameters (pass from the lambda code):
#    sns_topic_arn
#    environment
#    importer_type  (npi, rxnorm, etc)
#
#  Body parameters (pass from userdata body script):
#    message
#
#
#  NOTE: Python string interpolation using brackets, which conflicts with Bash.  To specify a Bash variable,
#        you must escape it like ${{var}}
#################################################################################################################

# Ensure on halt we do some cleanup and send out SNS message on completion.
function cleanup {{
  EXIT_CODE=$?
  set +e  # Do not exit immediately on error
  logger "Halting instance: Last received exit code=$EXIT_CODE, line=$1"

  # Direct link for cloudwatch logs
  cw_url="https://console.aws.amazon.com/cloudwatch/home?region=${{aws_region:-us-east-1}}#logEventViewer:group=/var/log/cloud-init-output.log;stream=${{instance_id}}"

  # Send status to SNS
  if [[ $EXIT_CODE -ne 0 ]]; then
    if [ -n "{sns_topic_arn}" ]; then
      aws --region ${{aws_region:-us-east-1}} sns publish \
            --topic-arn {sns_topic_arn} \
            --subject "{importer_type} {environment} failed." \
            --message "{importer_type} {environment} failed.  $cw_url" || true
    fi
  else
    if [ -n "{sns_topic_arn}" ]; then
      aws --region ${{aws_region:-us-east-1}} sns publish \
            --topic-arn {sns_topic_arn} \
            --subject "{importer_type} {environment} completed.  $message" \
            --message "{importer_type} {environment} completed in ${{total_time}} seconds.  $cw_url - $message" || true
    fi
  fi

  sleep 10  # give some extra time to get all logs to CW=

  if [[ "{terminate_on_completion}" == "true" ]]; then
    aws --region ${{aws_region:-us-east-1}} ec2 terminate-instances --instance-ids "${{instance_id}}"
  fi

  EXIT_CODE=$?
  sleep 180
  if [[ "$EXIT_CODE" -ne 0 ]]; then
    aws --region ${{aws_region:-us-east-1}} sns publish \
        --topic-arn {sns_topic_arn} \
        --subject "{importer_type} {environment} failed to terminate!" \
        --message "Failed to terminate {importer_type} {environment} EC2, exit code=$EXIT_CODE.  Attempting halt -p.  Please check instance_ID=$instance_id, $cw_url" || true
  fi
}}
trap 'cleanup $LINENO' EXIT

export aws_region=$(curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
export instance_id=$(curl http://169.254.169.254/latest/meta-data/instance-id)

# Send logs to CloudWatch
aws s3 cp s3://{bucket_name}/config/cloudwatch/awslogs.conf /etc/awslogs/awslogs.conf
systemctl start awslogsd

# Copy data package and runner package
aws s3 cp s3://{bucket_name}/importer.tar.gz /opt

# Install package
pip3 install /opt/importer.tar.gz
PATH=/usr/local/bin:$PATH

# For obtaining run time of body
export start=$(date +%s)

# Get database values
export loader_db_host=$(aws ssm get-parameters --names "/importer/{environment}/db_host" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_db_schema=$(aws ssm get-parameters --names "/importer/{environment}/db_schema" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_stage_db_schema=$(aws ssm get-parameters --names "/importer/{environment}/stage_db_schema" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
set +x   # don't print secrets
export loader_db_user=$(aws ssm get-parameters --names "/importer/{environment}/db_user" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_db_password=$(aws ssm get-parameters --names "/importer/{environment}/db_password" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)

# Setup .my.cnf file for any queries.
cat <<EOF > ~/.pgpass
$loader_db_host:*:*:$loader_db_user:$loader_db_password
EOF
chmod 600 ~/.pgpass

set -x

# Load our environment.  Used by runner.
# export aws_region=$(curl http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/[a-z]$//')
export aws_region=$(curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
export instance_id=$(curl http://169.254.169.254/latest/meta-data/instance-id)
export HOME=/root
