# serverless importer

## Install

```bash
# Configure environment
cp env.template .env.aws
vim .env.aws
# < edit values >
source .env.aws

# Deploy stack (RDS, Lambda, S3)
sls deploy --stage=dev

# Create initial database
sls invoke --function create_db

# Upload a (partial) data file to s3
bin/copy_data_files.sh npidata_pfile_50k.csv
```

This template creates RDS and S3 resources.

The step function isn't automatically created through the resources yet.  You will need to create the step function
manually with __resources/npi_state_machine.json__ and set the ARN to point to the __npi_step_importer__ function.

### Step Importer

Lambda + Step functions to import

Function name: npi_step_importer

Call step function inside the AWS console with the following:

```json
{
  "filename": "npidata_pfile_50k.csv",
  "chunk_size": 2500
}
```

TODO: Split the full file into smaller files.  Lambda has a 512MB filesystem limit and cannot download the full file at once.

### EC2 Importer

Deploys an ephemeral EC2 instance to handle data import.

** Not yet functional **


## Destroy

```bash
# S3 files must be removed, or Cloudformation cannot delete the bucket
bin/delete_bucket_files.sh

# Tear down stack
sls remove --stage=dev
```