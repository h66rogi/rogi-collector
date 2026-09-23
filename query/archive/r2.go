package archive

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var r2AccountIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// NewR2ObjectStore uses bucket-scoped R2 S3 API credentials. The endpoint is
// derived from the account ID rather than accepted from an untrusted request.
// The bucket remains private; public API clients never receive R2 credentials.
func NewR2ObjectStore(accountID, bucket, accessKeyID, secretAccessKey string) (*S3ObjectStore, error) {
	if !r2AccountIDPattern.MatchString(accountID) || bucket == "" || accessKeyID == "" || secretAccessKey == "" {
		return nil, errors.New("R2 account, bucket and S3 API credentials required")
	}
	credentials := aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: accessKeyID, SecretAccessKey: secretAccessKey, Source: "r2-bucket-token"}, nil
	}))
	client := s3.NewFromConfig(aws.Config{Region: "auto", Credentials: credentials}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID))
		options.UsePathStyle = true
	})
	return NewS3ObjectStore(client, bucket)
}
