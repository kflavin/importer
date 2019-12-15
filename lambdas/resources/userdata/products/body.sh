#################################################################################################################
# Userdata body.  Provide your own set of commands in the userdata body, which are specific to the import.
#################################################################################################################

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

# How many new product records were loaded?
products_count=$(mysql -D $loader_db_schema \
      -e "select count(*) from {table_name} where DATE(created_at)=DATE(NOW())" \
      -B -s -N)

      # How many new synonym records were loaded?
synonyms_count=$(mysql -D $loader_db_schema \
      -e "select count(*) from {synonyms_table_name} where DATE(created_at)=DATE(NOW())" \
      -B -s -N)

# message gets picked up by finish.sh
export message="$products_count new products, $synonyms_count new synonyms."