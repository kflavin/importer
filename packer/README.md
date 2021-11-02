# Packer Builder

This bakes an AMI that includes all the dependencies we need for the EC2's launched by the data-importer Lambda functions.

## Usage

The AMI id is set in your [environment file](https://github.com/rxvantage/data-importer/blob/master/env.template):

```bash
$ grep aws_image_id .env.dev
export aws_image_id='ami-0896df3262a030b69'        # Current AMI id
```

It should only need to be rebuilt if dependencies need to be updated.

## Build a New AMI

First, make sure you have [Packer](https://www.packer.io/) installed locally.

To build, export the following variables in your shell enviromnent:

```bash
export AWS_SOURCE_AMI=ami-02e136e904f3da870
export AWS_SUBNET_ID=subnet_id_here
export AWS_VPC_ID=vpc_id_here
```

We are using the `amzn2-ami-hvm-2.0.20211001.1-x86_64-gp2` as our base, with ami id: `ami-02e136e904f3da870`.

Once you've set your environment, run the following from the `packer/` directory:

```bash
packer validate loader.json
packer build loader.json
```

The resulting AMI ID can be added to your environment file.

_NOTE: bin/ directory has mysql binaries, so we can easily copy them into AWS, which only has MariaDB in their repos by default._
