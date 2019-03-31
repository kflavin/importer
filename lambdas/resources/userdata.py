user_data_tmpl = """#!/bin/bash
set -vxe
sleep 1

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
      aws --region ${{aws_region:-us-east-1}} sns publish --topic-arn {sns_topic_arn} --subject "{environment} {period} importer failed." --message "Importer EC2 failed.  Instance_ID=$instance_id, $cw_url" || true
    fi
  else
    if [ -n "{sns_topic_arn}" ]; then
      aws --region ${{aws_region:-us-east-1}} sns publish --topic-arn {sns_topic_arn} --subject "{environment} {period} importer completed." --message "Importer EC2 completed.  Instance_ID=$instance_id, $cw_url" || true
    fi
  fi

  sleep 10  # give some extra time to get all logs to CW=
  aws --region ${{aws_region:-us-east-1}} ec2 terminate-instances --instance-ids "${{instance_id}}"
  EXIT_CODE=$?
  sleep 180
  if [[ "$EXIT_CODE" -ne 0 ]]; then
    aws --region ${{aws_region:-us-east-1}} sns publish --topic-arn {sns_topic_arn} --subject "{environment} {period} importer failed to terminate!" --message "Failed to terminate instance, exit code=$EXIT_CODE.  Attempting halt -p.  Please check instance_ID=$instance_id, $cw_url" || true
  fi
}}
trap 'cleanup $LINENO' EXIT

# Load our environment.  Used by runner.
# export aws_region=$(curl http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/[a-z]$//')
export aws_region=$(curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
export instance_id=$(curl http://169.254.169.254/latest/meta-data/instance-id)
set +x
export db_user=$(aws ssm get-parameters --names "/importer/{environment}/db_user" --region ${{aws_region:-us-east-1}} --with-decryption --query Parameters[0].Value --output text)
export db_password=$(aws ssm get-parameters --names "/importer/{environment}/db_password" --region ${{aws_region:-us-east-1}} --with-decryption --query Parameters[0].Value --output text)
export db_host=$(aws ssm get-parameters --names "/importer/{environment}/db_host" --region ${{aws_region:-us-east-1}} --with-decryption --query Parameters[0].Value --output text)
export db_schema=$(aws ssm get-parameters --names "/importer/{environment}/db_schema" --region ${{aws_region:-us-east-1}} --with-decryption --query Parameters[0].Value --output text)
set -x

# Send logs to CloudWatch
aws s3 cp s3://{bucket_name}/config/awslogs.conf /etc/awslogs/awslogs.conf
systemctl start awslogsd

# Copy data package and runner package
aws s3 cp s3://{bucket_name}/importer.tar.gz /opt

# Install package
pip3 install /opt/importer.tar.gz
PATH=/usr/local/bin:$PATH

# Clean and load CSV file, then mark the object as imported
timeout {timeout}m runner-import.py -l cloudwatch npi full {init_flag} \
                    -t {table_name} \
                    -i {log_table_name} \
                    -p {period} \
                    -e {environment} \
                    -w /tmp/npi \
                    -u s3://{bucket_name}/{bucket_prefix} \
                    --limit {limit}

# Terminate the instance.  Give extra time for logs to sync.
sleep 10
"""
