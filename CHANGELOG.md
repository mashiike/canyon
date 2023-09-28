# Changelog

## [v0.4.1](https://github.com/mashiike/canyon/compare/v0.4.0...v0.4.1) - 2023-09-28
- change Serializer interface{} by @mashiike in https://github.com/mashiike/canyon/pull/26
- Buckup RequestBody for Serializer, maybe read RequestBody on handler by @mashiike in https://github.com/mashiike/canyon/pull/28
- delay seconds and Long delayed seconds with EventBridge Scheduler by @mashiike in https://github.com/mashiike/canyon/pull/29
- Add graceful failure handling for SQSMessage VisibilityTimeout by @mashiike in https://github.com/mashiike/canyon/pull/30

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
