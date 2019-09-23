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

# Clean and load CSV file, then mark the object as imported
timeout {timeout}m runner-import.py -l cloudwatch npi full {init_flag} \
                    -t {table_name} \
                    -i {log_table_name} \
                    -p {period} \
                    -e {environment} \
                    -w /tmp/npi \
                    -u s3://{bucket_name}/{bucket_prefix} \
                    --limit {limit}

# How many new records were loaded?
created=$(mysql -D $loader_db_schema \
      -e "select count(*) from {table_name} where DATE(created_at)=DATE(NOW())" \
      -B -s -N)

# How many new records were loaded?
updated=$(mysql -D $loader_db_schema \
      -e "select count(*) from {table_name} where DATE(updated_at)=DATE(NOW())" \
      -B -s -N)

# message gets picked up by finish.sh
export message="$created created. $updated updated."