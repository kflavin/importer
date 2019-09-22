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

fdisk /dev/nvme1n1 <<EOF
n
p
1


w

EOF

sleep 5

mkfs -t ext4 /dev/nvme1n1p1
mkdir /data
mount /dev/nvme1n1p1 /data
mysqldump --max-allowed-packet=1073741824 \
          --net-buffer-length=32704 \
          --single-transaction=TRUE \
          --skip-triggers \
          --set-gtid-purged=OFF \
          -h $loader_db_host \
          -u $loader_db_user \
          -p"$loader_db_password" \
          $loader_db_schema > /data/dump.sql

gzip /data/dump.sql

# Encrypt and upload
aws s3 cp --sse aws:kms /data/dump.sql.gz s3://importer-rc-dbbackupbucket-c6dx6gvyxji6/backups/$(date --iso-8601=seconds).sql.gz

size=$(ls -lh /data/dump.sql.gz  | awk '{{print $5}}')

# message gets picked up by finish.sh
export message="Uploaded $size backup"