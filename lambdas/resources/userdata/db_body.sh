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

#mysqldump -h $loader_db_host -u $loader_db_user -p$loader_db_password $loader_db_schema | aws s3 cp - s3://rxv-db-backups/db.sql
sleep 60

# message gets picked up by finish.sh
#export message="$products_count new products, $synonyms_count new synonyms."