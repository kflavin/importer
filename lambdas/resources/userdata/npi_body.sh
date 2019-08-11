#################################################################################################################
# Userdata body.  Provide your own set of commands in the userdata body, which are specific to the import.
#
# Lambda parameters:
#    bucket_prefix
#    environment
#    table_name: npis table
#    log_table_name: import log table
#    period: weekly|monthly
#    timeout: in minutes
#    init_flag: ONLY use for FIRST run!!
#    limit
#################################################################################################################

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

# Clean and load CSV file, then mark the object as imported
timeout {timeout}m runner-import.py -l cloudwatch npi full {init_flag} \
                    -t {table_name} \
                    -i {log_table_name} \
                    -p {period} \
                    -e {environment} \
                    -w /tmp/npi \
                    -u s3://{bucket_name}/{bucket_prefix} \
                    --limit {limit}

