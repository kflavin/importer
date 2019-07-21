export loader_db_host=$(aws ssm get-parameters --names "/importer/{environment}/db_host" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_db_schema=$(aws ssm get-parameters --names "/importer/{environment}/db_schema" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_stage_db_schema=$(aws ssm get-parameters --names "/importer/{environment}/stage_db_schema" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
set +x   # don't print secrets
export loader_db_user=$(aws ssm get-parameters --names "/importer/{environment}/db_user" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_db_password=$(aws ssm get-parameters --names "/importer/{environment}/db_password" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
set -x

mkdir -p /home/ec2-user/rxnorm_jobs /home/ec2-user/talend /home/ec2-user/.talend/context

# Get config file
aws s3 cp s3://{bucket_name}/config/talend/template.cfg "/home/ec2-user/.talend/context/{environment}.cfg"

# Get Talend jar
aws s3 cp s3://{bucket_name}/RxNorm_Loader.zip /home/ec2-user/rxnorm_jobs
cd /home/ec2-user/rxnorm_jobs
unzip RxNorm_Loader.zip
cd RxNorm_Loader/

sleep 180

# Download any new files
timeout {timeout}m runner-import.py products download all # >> /home/ec2-user/jobs/logs/downloader.log 2>&1

# Load stage tables, and call SP
timeout {timeout}m bash RxNorm_Loader_run.sh --context_param contextName="{environment}" # 2>&1 | tee -a $LOG_FILE