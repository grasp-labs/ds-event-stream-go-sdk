module producer-example

go 1.23.0

require (
	github.com/aws/aws-sdk-go-v2/config v1.31.18
	github.com/aws/aws-sdk-go-v2/service/ssm v1.67.0
	github.com/google/uuid v1.6.0
	github.com/grasp-labs/ds-event-stream-go-sdk v0.0.0
)

require (
	github.com/aws/aws-sdk-go-v2 v1.39.6 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.22 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.40.0 // indirect
	github.com/aws/smithy-go v1.23.2 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/segmentio/kafka-go v0.4.49 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	golang.org/x/text v0.23.0 // indirect
)

replace github.com/grasp-labs/ds-event-stream-go-sdk => ../..
