package option

import "github.com/aws/aws-sdk-go/service/s3"

// The HeadObjectInput type is an adapter to change a parameter in
// s3.HeadObjectInput.
type HeadObjectInput func(req *s3.HeadObjectInput)
