# Changelog

## [v0.11.0](https://github.com/mashiike/canyon/compare/v0.10.0...v0.11.0) - 2025-04-18
- Add UsedFromContext and IsWorkerFromContext by @mashiike in https://github.com/mashiike/canyon/pull/180
- Feature/fix ci by @mashiike in https://github.com/mashiike/canyon/pull/189
- Feature/streaming response by @mashiike in https://github.com/mashiike/canyon/pull/191

## [v0.10.0](https://github.com/mashiike/canyon/compare/v0.9.0...v0.10.0) - 2025-02-10
- Add Lambda middleware interface by @mashiike in https://github.com/mashiike/canyon/pull/178

## [v0.9.0](https://github.com/mashiike/canyon/compare/v0.8.1...v0.9.0) - 2025-02-07
- implement WithSignalTERM by @mashiike in https://github.com/mashiike/canyon/pull/176
- Bump github.com/stretchr/testify from 1.9.0 to 1.10.0 by @dependabot in https://github.com/mashiike/canyon/pull/164

## [v0.8.1](https://github.com/mashiike/canyon/compare/v0.8.0...v0.8.1) - 2024-11-20
- Bump support go version and go moduels by @mashiike in https://github.com/mashiike/canyon/pull/160
- Bump github.com/stretchr/testify from 1.8.4 to 1.9.0 by @dependabot in https://github.com/mashiike/canyon/pull/162

## [v0.8.0](https://github.com/mashiike/canyon/compare/v0.7.1...v0.8.0) - 2024-07-18
- when development, change inmemory visibility timeout by @mashiike in https://github.com/mashiike/canyon/pull/135

## [v0.7.1](https://github.com/mashiike/canyon/compare/v0.7.0...v0.7.1) - 2024-02-22
- Bump github.com/aws/aws-sdk-go-v2/credentials from 1.16.15 to 1.16.16 by @dependabot in https://github.com/mashiike/canyon/pull/88
- Bump github.com/aws/aws-sdk-go-v2/config from 1.26.4 to 1.27.0 by @dependabot in https://github.com/mashiike/canyon/pull/94
- Bump github.com/google/uuid from 1.5.0 to 1.6.0 by @dependabot in https://github.com/mashiike/canyon/pull/91
- Bump github.com/aws/aws-sdk-go-v2/service/s3 from 1.48.0 to 1.48.1 by @dependabot in https://github.com/mashiike/canyon/pull/92

## [v0.7.0](https://github.com/mashiike/canyon/compare/v0.6.0...v0.7.0) - 2024-01-17
- add util function for check runs on canyon by @mashiike in https://github.com/mashiike/canyon/pull/40
- Bump actions/setup-go from 4 to 5 by @dependabot in https://github.com/mashiike/canyon/pull/71
- fix CI golang-ci lint by @mashiike in https://github.com/mashiike/canyon/pull/77
- for API Gateway Websocket API by @mashiike in https://github.com/mashiike/canyon/pull/78
- Bump github.com/aws/aws-sdk-go-v2/feature/s3/manager from 1.15.11 to 1.15.12 by @dependabot in https://github.com/mashiike/canyon/pull/84

## [v0.6.0](https://github.com/mashiike/canyon/compare/v0.5.0...v0.6.0) - 2023-09-30
- Add support for Retry-After header in worker by @mashiike in https://github.com/mashiike/canyon/pull/32
- fix ChangeMessageVisibility by @mashiike in https://github.com/mashiike/canyon/pull/34
- Updated case conversion to consider common initialisms by @mashiike in https://github.com/mashiike/canyon/pull/35

## [v0.5.0](https://github.com/mashiike/canyon/compare/v0.4.0...v0.5.0) - 2023-09-28
- change Serializer interface{} by @mashiike in https://github.com/mashiike/canyon/pull/26
- Buckup RequestBody for Serializer, maybe read RequestBody on handler by @mashiike in https://github.com/mashiike/canyon/pull/28
- delay seconds and Long delayed seconds with EventBridge Scheduler by @mashiike in https://github.com/mashiike/canyon/pull/29

## [v0.4.0](https://github.com/mashiike/canyon/compare/v0.3.1...v0.4.0) - 2023-09-26
- fallback handler can send sqs message by @mashiike in https://github.com/mashiike/canyon/pull/19
- WithSerializer option by @mashiike in https://github.com/mashiike/canyon/pull/25

## [v0.3.1](https://github.com/mashiike/canyon/compare/v0.3.0...v0.3.1) - 2023-09-09
- fix Lambda fallback  by @mashiike in https://github.com/mashiike/canyon/pull/15
- on Lambda Runtime: if use in memory queue,sync processing by @mashiike in https://github.com/mashiike/canyon/pull/17

## [v0.3.0](https://github.com/mashiike/canyon/compare/v0.2.0...v0.3.0) - 2023-09-09
- fix misc by @mashiike in https://github.com/mashiike/canyon/pull/11
- Lambda Fallback Handler  by @mashiike in https://github.com/mashiike/canyon/pull/14

## [v0.2.0](https://github.com/mashiike/canyon/compare/v0.1.0...v0.2.0) - 2023-09-08
- rename SendToSQS => SendToWorker by @mashiike in https://github.com/mashiike/canyon/pull/6
- s3 backend by @mashiike in https://github.com/mashiike/canyon/pull/8
- canyontest is test helper package  by @mashiike in https://github.com/mashiike/canyon/pull/9
- for local development helpfull option by @mashiike in https://github.com/mashiike/canyon/pull/10

## [v0.1.0](https://github.com/mashiike/canyon/commits/v0.1.0) - 2023-09-07
- Bump actions/checkout from 3 to 4 by @dependabot in https://github.com/mashiike/canyon/pull/1
- Bump github.com/stretchr/testify from 1.7.2 to 1.8.4 by @dependabot in https://github.com/mashiike/canyon/pull/3
- Bump github.com/pires/go-proxyproto from 0.6.0 to 0.7.0 by @dependabot in https://github.com/mashiike/canyon/pull/4
