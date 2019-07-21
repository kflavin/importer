#!/bin/bash
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
      aws --region ${{aws_region:-us-east-1}} sns publish --topic-arn {sns_topic_arn} --subject "{importer_type} {environment} importer failed." --message "{environment} importer failed after ${{total_time}} seconds.  Instance_ID=$instance_id, $cw_url" || true
    fi
  else
    if [ -n "{sns_topic_arn}" ]; then
      aws --region ${{aws_region:-us-east-1}} sns publish --topic-arn {sns_topic_arn} --subject "{importer_type} {environment} importer completed." --message "{environment} importer completed in ${{total_time}} seconds.  Instance_ID=$instance_id, $cw_url" || true
    fi
  fi

  sleep 10  # give some extra time to get all logs to CW=
  aws --region ${{aws_region:-us-east-1}} ec2 terminate-instances --instance-ids "${{instance_id}}"
  EXIT_CODE=$?
  sleep 180
  if [[ "$EXIT_CODE" -ne 0 ]]; then
    aws --region ${{aws_region:-us-east-1}} sns publish --topic-arn {sns_topic_arn} --subject "{importer_type} {environment} importer failed to terminate!" --message "Failed to terminate {importer_type} {environment} EC2, exit code=$EXIT_CODE.  Attempting halt -p.  Please check instance_ID=$instance_id, $cw_url" || true
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