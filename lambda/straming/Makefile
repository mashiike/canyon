export AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query 'Account' --output text)
.PHONY: clean lambroll/logs lambroll/deploy

bootstrap: *.go
	GOOS=linux GOARCH=amd64 go build -o bootstrap main.go

clean:
	rm -f bootstrap
	rm -f canyon_example_dummy.zip

lambroll/deploy: bootstrap
	lambroll --log-level debug deploy --function-url function_url.json
	$(MAKE) clean

lambroll/logs:
	lambroll logs --follow --format=short

