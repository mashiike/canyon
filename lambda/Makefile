
.PHONY: terraform/init
terraform/init:
	terraform providers lock \
		-platform=darwin_amd64 \
		-platform=darwin_arm64 \
		-platform=linux_amd64 \
		-platform=linux_arm64 \
		-platform=windows_amd64
	terraform init


.PHONY: terraform/plan
terraform/plan:
	terraform plan


.PHONY: terraform/apply
terraform/apply:
	terraform apply
