# Packer Builder

First set the following values in your environment:
```
export AWS_SOURCE_AMI=ami-b70554c8
export AWS_SUBNET_ID=<subnet_id>
export AWS_VPC_ID=<vpc id>
```

To build the custom AMI for the loader:

```bash
packer validate loader.json
packer build loader.json
```

Add the resulting AMI ID into your environment file.

NOTE: bin/ directory has mysql binaries, so we can easily copy them into AWS, which only has MariaDB in their repos by default.
