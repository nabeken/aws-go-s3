package option

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// The CopyObjectInput type is an adapter to change a parameter in
// s3.CopyObjectInput.
type CopyObjectInput func(req *s3.CopyObjectInput)

// CopySSEKMSKeyID returns a CopyObjectInput that changes a SSE-KMS Key ID.
func CopySSEKMSKeyID(keyID string) CopyObjectInput {
	return func(req *s3.CopyObjectInput) {
		req.SSEKMSKeyId = aws.String(keyID)
		req.ServerSideEncryption = aws.String("aws:kms")
	}
}
