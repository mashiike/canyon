export AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query 'Account' --output text)
.PHONY: clean lambroll/logs lambroll/deploy

bootstrap: *.go
	GOOS=linux GOARCH=amd64 go build -o bootstrap main.go

clean:
	rm -f bootstrap
	rm -f canyon_example_dummy.zip

lambroll/deploy: bootstrap
	lambroll --log-level debug deploy

lambroll/logs:
	lambroll logs --follow --format=short


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
