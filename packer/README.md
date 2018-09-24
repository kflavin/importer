# Packer Builder

To build the custom AMI for the loader:

```bash
packer validate importer.json
packer build importer.json
```

Add the resulting AMI ID into your environment file.