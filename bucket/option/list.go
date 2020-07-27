package option

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// The ListObjectsInput type is an adapter to change a parameter in
// s3.ListObjectsInput.
type ListObjectsInput func(req *s3.ListObjectsInput)

// The ListObjectsV2Input type is an adapter to change a parameter in
// s3.ListObjectsV2Input.
type ListObjectsV2Input func(req *s3.ListObjectsV2Input)

// The ListObjectVersionsInput type is an adapter to change a parameter in
// s3.ListObjectVersionsInput.
type ListObjectVersionsInput func(req *s3.ListObjectVersionsInput)

// ListDelimiter returns a ListObjectsInput that changes a delimiter in
// s3.ListObjectsInput.
func ListDelimiter(delim string) ListObjectsInput {
	return func(req *s3.ListObjectsInput) {
		req.Delimiter = aws.String(delim)
	}
}

// ListEncodingType returns a ListObjectsInput that changes a EncodingType in
// s3.ListObjectsInput.
func ListEncodingType(typ string) ListObjectsInput {
	return func(req *s3.ListObjectsInput) {
		req.EncodingType = aws.String(typ)
	}
}

// ListMarker returns a ListObjectsInput that changes a Marker in
// s3.ListObjectsInput.
func ListMarker(marker string) ListObjectsInput {
	return func(req *s3.ListObjectsInput) {
		req.Marker = aws.String(marker)
	}
}
