#################################################################################################################
# Userdata body.  Provide your own set of commands in the userdata body, which are specific to the import.
#################################################################################################################

# Parition the disk.  Initially I tried to stream the dump directly into S3, but it wasn't working; the MySQL
# connection kept dropping.  This seems to be more reliable.
fdisk /dev/nvme1n1 <<EOF
n
p
1


w

EOF

# Sleep for a few seconds to ensure the partition is available.
sleep 5

mkfs -t ext4 /dev/nvme1n1p1
mkdir /data
mount /dev/nvme1n1p1 /data

# Increased packet size helps with the disconnects.  Changed in RDS as well to match the value listed here.
mysqldump --max-allowed-packet=1073741824 \
          --net-buffer-length=32704 \
          --single-transaction=TRUE \
          --skip-triggers \
          --set-gtid-purged=OFF \
          $loader_db_schema > /data/dump.sql

gzip /data/dump.sql

# Encrypt and upload
aws s3 cp --sse aws:kms /data/dump.sql.gz s3://{backup_bucket_name}/backups/$(date --iso-8601=seconds).sql.gz

size=$(ls -lh /data/dump.sql.gz  | awk '{{print $5}}')

# message gets picked up by finish.sh
export message="Uploaded $size backup"