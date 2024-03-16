package option

import "github.com/aws/aws-sdk-go-v2/service/s3"

// The GetObjectInput type is an adapter to change a parameter in
// s3.GetObjectInput.
type GetObjectInput func(req *s3.GetObjectInput)
