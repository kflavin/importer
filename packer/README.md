# Packer Builder

First set the following values in your environment:
```
export AWS_SOURCE_AMI=ami-b70554c8
export AWS_SUBNET_ID=<subnet_id>
export AWS_VPC_ID=<vpc id>
```

To build the custom AMI for the loader:

```bash
packer validate importer.json
packer build importer.json
```

Add the resulting AMI ID into your environment file.
