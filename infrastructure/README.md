# Collector AWS root

This root consumes the shared network IDs and `marble_security_group_id`; it does not own shared networking. It creates one collector EC2 host, retained encrypted data EBS, SSM role, empty secret containers, and backup bucket. Keep state, plans, backend configuration, AMI IDs, account values, and tfvars outside Git. Terraform does not format EBS or populate secrets. See [infrastructure preparation](../docs/infrastructure-preparation.md).

`terraform test` uses the AWS mock provider. Its `apply` runs only against in-memory mock resources and never contacts or mutates AWS. Credentialed saved plans and every real apply remain approved-operator actions outside this repository.
