package bucket

import (
	"context"
	"io"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/nabeken/aws-go-s3/bucket/option"
)

// A Bucket is an S3 bucket which holds properties such as bucket name and SSE things for S3 Bucket.
type Bucket struct {
	S3   s3iface.S3API
	Name *string
}

// New returns Bucket instance with bucket name name.
func New(s s3iface.S3API, name string) *Bucket {
	return &Bucket{
		S3:   s,
		Name: aws.String(name),
	}
}

// GetObject wraps GetObjectWithContext using context.Background.
func (b *Bucket) GetObject(key string, opts ...option.GetObjectInput) (*s3.GetObjectOutput, error) {
	return b.GetObjectWithContext(context.Background(), key, opts...)
}

// GetObjectWithContext returns the s3.GetObjectOutput.
func (b *Bucket) GetObjectWithContext(ctx context.Context, key string, opts ...option.GetObjectInput) (*s3.GetObjectOutput, error) {
	req := &s3.GetObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.GetObjectWithContext(ctx, req)
}

// GetObjectReader wraps GetObjectReaderWithContext using context.Background.
func (b *Bucket) GetObjectReader(key string, opts ...option.GetObjectInput) (io.ReadCloser, error) {
	return b.GetObjectReaderWithContext(context.Background(), key, opts...)
}

// GetObjectReaderWithContext returns a reader assosiated with body. A caller of this MUST close the reader when it finishes reading.
func (b *Bucket) GetObjectReaderWithContext(ctx context.Context, key string, opts ...option.GetObjectInput) (io.ReadCloser, error) {
	resp, err := b.GetObjectWithContext(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// GetObjectRequest generates a "aws/request.Request" representing the client's request for the GetObject operation.
func (b *Bucket) GetObjectRequest(key string, opts ...option.GetObjectInput) (*request.Request, *s3.GetObjectOutput) {
	req := &s3.GetObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.GetObjectRequest(req)
}

// HeadObject wraps HeadObjectWithContext using context.Background.
func (b *Bucket) HeadObject(key string, opts ...option.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return b.HeadObjectWithContext(context.Background(), key, opts...)
}

// HeadObjectWithContext retrieves an object metadata for key.
func (b *Bucket) HeadObjectWithContext(ctx context.Context, key string, opts ...option.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	req := &s3.HeadObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.HeadObjectWithContext(ctx, req)
}

// ExistsObject wraps ExistsObjectWithContext using context.Background.
func (b *Bucket) ExistsObject(key string, opts ...option.HeadObjectInput) (bool, error) {
	return b.ExistsObjectWithContext(context.Background(), key, opts...)
}

// ExistsObjectWithContext returns true if key does not exist on bucket.
func (b *Bucket) ExistsObjectWithContext(ctx context.Context, key string, opts ...option.HeadObjectInput) (bool, error) {
	_, err := b.HeadObjectWithContext(ctx, key, opts...)
	if err == nil {
		return true, nil
	}

	if s3err, ok := err.(awserr.RequestFailure); ok && s3err.StatusCode() == http.StatusNotFound {
		// actually key does not exist
		return false, nil
	}

	// in some error situation
	return false, err
}

// PutObject puts an object with reading data from reader.
func (b *Bucket) PutObject(key string, rs io.ReadSeeker, opts ...option.PutObjectInput) (*s3.PutObjectOutput, error) {
	return b.PutObjectWithContext(context.Background(), key, rs, opts...)
}

// PutObjectWithContext puts an object with reading data from reader.
func (b *Bucket) PutObjectWithContext(ctx context.Context, key string, rs io.ReadSeeker, opts ...option.PutObjectInput) (*s3.PutObjectOutput, error) {
	req := &s3.PutObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
		Body:   rs,
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.PutObjectWithContext(ctx, req)
}

// DeleteObject wraps DeleteObjectWithContext using context.Background.
func (b *Bucket) DeleteObject(key string) (*s3.DeleteObjectOutput, error) {
	return b.DeleteObjectWithContext(context.Background(), key)
}

// DeleteObjectWithContext deletes an object for key.
func (b *Bucket) DeleteObjectWithContext(ctx context.Context, key string) (*s3.DeleteObjectOutput, error) {
	req := &s3.DeleteObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	return b.S3.DeleteObjectWithContext(ctx, req)
}

// DeleteObjects wraps DeleteObjectsWithContext using context.Background.
func (b *Bucket) DeleteObjects(identifiers []*s3.ObjectIdentifier) (*s3.DeleteObjectsOutput, error) {
	return b.DeleteObjectsWithContext(context.Background(), identifiers)
}

// DeleteObjectsWithContext deletes each object for the given identifiers.
// A maximum of 1000 objects can be deleted at a time with this method.
func (b *Bucket) DeleteObjectsWithContext(ctx context.Context, identifiers []*s3.ObjectIdentifier) (*s3.DeleteObjectsOutput, error) {
	req := &s3.DeleteObjectsInput{
		Bucket: b.Name,
		Delete: &s3.Delete{
			Objects: identifiers,
		},
	}

	return b.S3.DeleteObjectsWithContext(ctx, req)
}

// ListObjects wraps ListObjectsWithContext using.
func (b *Bucket) ListObjects(prefix string, opts ...option.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	return b.ListObjectsWithContext(context.Background(), prefix, opts...)
}

// ListObjectsWithContext lists objects that has prefix.
func (b *Bucket) ListObjectsWithContext(ctx context.Context, prefix string, opts ...option.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	req := &s3.ListObjectsInput{
		Bucket: b.Name,
		Prefix: aws.String(prefix),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.ListObjectsWithContext(ctx, req)
}

// ListObjectsV2PagesWithContext will page through objects with the given prefix.
func (b *Bucket) ListObjectsV2PagesWithContext(
	ctx aws.Context,
	prefix string,
	pageFunc func(*s3.ListObjectsV2Output, bool) bool,
	opts ...option.ListObjectsV2Input,
) error {
	req := &s3.ListObjectsV2Input{
		Bucket: b.Name,
		Prefix: aws.String(prefix),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.ListObjectsV2PagesWithContext(ctx, req, pageFunc)
}

// ListObjectVersionsPagesWithContext will page through all versions of all objects with the given prefix.
func (b *Bucket) ListObjectVersionsPagesWithContext(
	ctx aws.Context,
	prefix string,
	pageFunc func(*s3.ListObjectVersionsOutput, bool) bool,
	opts ...option.ListObjectVersionsInput,
) error {
	req := &s3.ListObjectVersionsInput{
		Bucket: b.Name,
		Prefix: aws.String(prefix),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.ListObjectVersionsPagesWithContext(ctx, req, pageFunc)
}

// CopyObject wraps CopyObjectWithContext using context.Background.
func (b *Bucket) CopyObject(dest, src string, opts ...option.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	return b.CopyObjectWithContext(context.Background(), dest, src, opts...)
}

// CopyObjectWithContext copies an object within the bucket.
func (b *Bucket) CopyObjectWithContext(ctx context.Context, dest, src string, opts ...option.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	req := &s3.CopyObjectInput{
		Bucket:     b.Name,
		Key:        aws.String(dest),
		CopySource: aws.String(aws.StringValue(b.Name) + "/" + url.QueryEscape(src)),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.CopyObjectWithContext(ctx, req)
}
