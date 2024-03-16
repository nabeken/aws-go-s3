package bucket

import (
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nabeken/aws-go-s3/v2/bucket/option"
)

// A Bucket is an S3 bucket which holds properties such as bucket name and SSE things for S3 Bucket.
type Bucket struct {
	S3   *s3.Client
	Name *string
}

// New returns Bucket instance with bucket name name.
func New(s *s3.Client, name string) *Bucket {
	return &Bucket{
		S3:   s,
		Name: aws.String(name),
	}
}

// GetObject returns the s3.GetObjectOutput.
func (b *Bucket) GetObject(ctx context.Context, key string, opts ...option.GetObjectInput) (*s3.GetObjectOutput, error) {
	req := &s3.GetObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.GetObject(ctx, req)
}

// GetObjectReader returns a reader assosiated with body. A caller of this MUST close the reader when it finishes reading.
func (b *Bucket) GetObjectReader(ctx context.Context, key string, opts ...option.GetObjectInput) (io.ReadCloser, error) {
	resp, err := b.GetObject(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// HeadObject retrieves an object metadata for key.
func (b *Bucket) HeadObject(ctx context.Context, key string, opts ...option.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	req := &s3.HeadObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.HeadObject(ctx, req)
}

// ExistsObject returns true if key does not exist on bucket.
func (b *Bucket) ExistsObject(ctx context.Context, key string, opts ...option.HeadObjectInput) (bool, error) {
	_, err := b.HeadObject(ctx, key, opts...)
	if err == nil {
		return true, nil
	}

	var ne *types.NotFound
	if errors.As(err, &ne) {
		// actually key does not exist
		return false, nil
	}

	// in some error situation
	return false, err
}

// PutObject puts an object with reading data from reader.
func (b *Bucket) PutObject(ctx context.Context, key string, rs io.ReadSeeker, opts ...option.PutObjectInput) (*s3.PutObjectOutput, error) {
	req := &s3.PutObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
		Body:   rs,
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.PutObject(ctx, req)
}

// DeleteObject deletes an object for key.
func (b *Bucket) DeleteObject(ctx context.Context, key string) (*s3.DeleteObjectOutput, error) {
	req := &s3.DeleteObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	return b.S3.DeleteObject(ctx, req)
}

// DeleteObjects deletes each object for the given identifiers.
// A maximum of 1000 objects can be deleted at a time with this method.
func (b *Bucket) DeleteObjects(ctx context.Context, identifiers []types.ObjectIdentifier) (*s3.DeleteObjectsOutput, error) {
	req := &s3.DeleteObjectsInput{
		Bucket: b.Name,
		Delete: &types.Delete{
			Objects: identifiers,
		},
	}

	return b.S3.DeleteObjects(ctx, req)
}

// ListObjects lists objects that has prefix.
func (b *Bucket) ListObjects(ctx context.Context, prefix string, opts ...option.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	req := &s3.ListObjectsInput{
		Bucket: b.Name,
		Prefix: aws.String(prefix),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.ListObjects(ctx, req)
}

// ListObjectsV2PaginatorFactory represents a factory that builds *s3.ListObjectsV2Paginator.
type ListObjectsV2PaginatorFactory func(opts ...func(*s3.ListObjectsV2PaginatorOptions)) *s3.ListObjectsV2Paginator

// BuildListObjectsV2PaginatorFactory builds a factory that builds *s3.ListObjectsV2Paginator to go through objects with a given prefix.
func (b *Bucket) BuildListObjectsV2PaginatorFactory(
	ctx context.Context,
	prefix string,
	opts ...option.ListObjectsV2Input,
) ListObjectsV2PaginatorFactory {
	req := &s3.ListObjectsV2Input{
		Bucket: b.Name,
		Prefix: aws.String(prefix),
	}

	for _, f := range opts {
		f(req)
	}

	return func(opts ...func(*s3.ListObjectsV2PaginatorOptions)) *s3.ListObjectsV2Paginator {
		return s3.NewListObjectsV2Paginator(b.S3, req, opts...)
	}
}

// ListObjectVersionsPaginatorFactory represents a factory that builds *s3.ListObjectVersionsPaginator.
type ListObjectVersionsPaginatorFactory func(opts ...func(*s3.ListObjectVersionsPaginatorOptions)) *s3.ListObjectVersionsPaginator

// BuildListObjectVersionsPaginatorFactory builds a factory that builds *s3.ListObjectVersionsPaginator to go through versioning objects with a given prefix.
func (b *Bucket) BuildListObjectVersionsPaginatorFactory(
	ctx context.Context,
	prefix string,
	opts ...option.ListObjectVersionsInput,
) ListObjectVersionsPaginatorFactory {
	req := &s3.ListObjectVersionsInput{
		Bucket: b.Name,
		Prefix: aws.String(prefix),
	}

	for _, f := range opts {
		f(req)
	}

	return func(opts ...func(*s3.ListObjectVersionsPaginatorOptions)) *s3.ListObjectVersionsPaginator {
		return s3.NewListObjectVersionsPaginator(b.S3, req, opts...)
	}
}

// CopyObject copies an object within the bucket.
func (b *Bucket) CopyObject(ctx context.Context, dest, src string, opts ...option.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	req := &s3.CopyObjectInput{
		Bucket:     b.Name,
		Key:        aws.String(dest),
		CopySource: aws.String(aws.ToString(b.Name) + "/" + url.QueryEscape(src)),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.CopyObject(ctx, req)
}

// PresignClient generates a presign client wrapper using the given API Client and presign options.
func (b *Bucket) PresignClient(optFns ...func(*s3.PresignOptions)) *PresignClient {
	return &PresignClient{
		PresignClient: s3.NewPresignClient(b.S3, optFns...),
		Name:          b.Name,
		optFns:        optFns,
	}
}

// PresignClient wraps the S3 presign client with the given S3 API client, bucket name and the presign optionsd.
// If you want to give different presign options, please generate another one by Bucket#PresignClient.
// It implements only the object-level operations.
type PresignClient struct {
	PresignClient *s3.PresignClient
	Name          *string
	optFns        []func(*s3.PresignOptions)
}

// PresignPutObject generates a presigned HTTP Request for PutObject operation.
func (c *PresignClient) PresignPutObject(ctx context.Context, key string, rs io.ReadSeeker, opts ...option.PutObjectInput) (*v4.PresignedHTTPRequest, error) {
	req := &s3.PutObjectInput{
		Bucket: c.Name,
		Key:    aws.String(key),
		Body:   rs,
	}

	for _, f := range opts {
		f(req)
	}

	return c.PresignClient.PresignPutObject(ctx, req, c.optFns...)
}

// PresignGetObject generates a presigned HTTP Request for GetObject operation.
func (c *PresignClient) PresignGetObject(ctx context.Context, key string, opts ...option.GetObjectInput) (*v4.PresignedHTTPRequest, error) {
	req := &s3.GetObjectInput{
		Bucket: c.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return c.PresignClient.PresignGetObject(ctx, req, c.optFns...)
}

// PresignHeadObject generates a presigned HTTP Request for HeadObject operation.
func (c *PresignClient) PresignHeadObject(ctx context.Context, key string, opts ...option.HeadObjectInput) (*v4.PresignedHTTPRequest, error) {
	req := &s3.HeadObjectInput{
		Bucket: c.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return c.PresignClient.PresignHeadObject(ctx, req, c.optFns...)
}

// PresignDeleteObject generates a presigned HTTP Request for DeleteObject operation.
func (c *PresignClient) PresignDeleteObject(ctx context.Context, key string) (*v4.PresignedHTTPRequest, error) {
	req := &s3.DeleteObjectInput{
		Bucket: c.Name,
		Key:    aws.String(key),
	}

	return c.PresignClient.PresignDeleteObject(ctx, req, c.optFns...)
}
