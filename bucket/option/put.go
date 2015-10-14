package option

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// The PutObjectInput type is an adapter to change a parameter in
// s3.PutObjectInput.
type PutObjectInput func(req *s3.PutObjectInput)

// SSEKMSKeyID returns a PutObjectInput that changes a SSE-KMS Key ID.
func SSEKMSKeyID(keyID string) PutObjectInput {
	return func(req *s3.PutObjectInput) {
		req.SSEKMSKeyId = aws.String(keyID)
	}
}

// ACLPrivate returns a PutObjectInput that set ACL private.
func ACLPrivate() PutObjectInput {
	return func(req *s3.PutObjectInput) {
		req.ACL = aws.String(s3.ObjectCannedACLPrivate)
	}
}

// ContentType returns a PutObjectInput that set Content-Type.
func ContentType(ct string) PutObjectInput {
	return func(req *s3.PutObjectInput) {
		req.ContentType = aws.String(ct)
	}
}

// ContentLength returns a PutObjectInput that set Content-Length.
func ContentLength(length int64) PutObjectInput {
	return func(req *s3.PutObjectInput) {
		req.ContentLength = aws.Int64(length)
	}
}
