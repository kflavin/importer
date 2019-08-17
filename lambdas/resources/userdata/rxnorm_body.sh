#################################################################################################################
# Userdata body.  Provide your own set of commands in the userdata body, which are specific to the import.
#################################################################################################################
export loader_db_host=$(aws ssm get-parameters --names "/importer/{environment}/db_host" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_db_schema=$(aws ssm get-parameters --names "/importer/{environment}/db_schema" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_stage_db_schema=$(aws ssm get-parameters --names "/importer/{environment}/stage_db_schema" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
set +x   # don't print secrets
export loader_db_user=$(aws ssm get-parameters --names "/importer/{environment}/db_user" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
export loader_db_password=$(aws ssm get-parameters --names "/importer/{environment}/db_password" --region "${{aws_region:-us-east-1}}" --with-decryption --query Parameters[0].Value --output text)
set -x

mkdir -p /root/rxnorm_jobs /root/talend /root/.talend/context

# Get config file
aws s3 cp s3://{bucket_name}/config/talend/template.cfg "/root/.talend/context/{environment}.cfg"

# Get Talend jar
aws s3 cp s3://{bucket_name}/RxNorm_Loader.zip /root/rxnorm_jobs
cd /root/rxnorm_jobs
unzip RxNorm_Loader.zip
cd RxNorm_Loader/

sleep 60

# Download any new files
timeout {timeout}m runner-import.py products download all # >> /root/jobs/logs/downloader.log 2>&1

# Load stage tables, and call SP
timeout {timeout}m bash RxNorm_Loader_run.sh --context_param contextName="{environment}" # 2>&1 | tee -a $LOG_FILE

# How many new records were loaded?
count=$(mysql -h $loader_db_host \
      -u $loader_db_user \
      -p$loader_db_password \
      $loader_db_schema \
      -e "select count(*) from products where DATE(created_at)=DATE(NOW())" \
      -B -s -N)

export message="$count new products."