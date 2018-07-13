# serverless importer

## Install

```bash
cp env.template .env.aws
# < edit values >
sls deploy --stage=dev

# Create initial database
sls invoke --function create_db

# Upload a (partial) data file to s3
bin/copy_data_files.sh npidata_pfile_50k.csv
```

This template creates RDS and S3 resources.

The step function isn't automatically created through the resources yet.  You will need to create the step function
manually with __resources/npi_state_machine.json__ and set the ARN to point to the __npi_step_importer__ function.

## Step Importer

Lambda + Step functions to import

Function name: npi_step_importer

Call from step function with the following:

```json
{
  "filename": "npidata_pfile_50k.csv",
  "chunk_size": 2500
}
```


## EC2 Importer

Deploys an ephemeral EC2 instance to handle data import.

** Not yet functional **