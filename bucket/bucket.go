package bucket

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
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

// GetObjectReader returns a reader assosiated with body. A caller of this MUST close the reader when it finishes reading.
func (b *Bucket) GetObjectReader(key string, opts ...option.GetObjectInput) (io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	resp, err := b.S3.GetObject(req)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// HeadObject retrieves an object metadata for key.
func (b *Bucket) HeadObject(key string, opts ...option.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	req := &s3.HeadObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.HeadObject(req)
}

// PutObject puts an object with reading data from reader.
func (b *Bucket) PutObject(key string, rs io.ReadSeeker, opts ...option.PutObjectInput) (*s3.PutObjectOutput, error) {
	req := &s3.PutObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
		Body:   rs,
	}

	for _, f := range opts {
		f(req)
	}

	return b.S3.PutObject(req)
}

// DeleteObject deletes an object for key.
func (b *Bucket) DeleteObject(key string) (*s3.DeleteObjectOutput, error) {
	req := &s3.DeleteObjectInput{
		Bucket: b.Name,
		Key:    aws.String(key),
	}

	return b.S3.DeleteObject(req)
}

// FileReadSeeker is tempfile-based io.ReadSeeker implementation.
type FileReadSeeker struct {
	file *os.File
}

// Close closes underlying tempfile and remove it.
func (f *FileReadSeeker) Close() error {
	if err := f.file.Close(); err != nil {
		return err
	}

	return os.Remove(f.file.Name())
}

// Read implements io.Reader with underlying tempfile.
func (f *FileReadSeeker) Read(p []byte) (int, error) {
	return f.file.Read(p)
}

// Seek implements io.Seeker with underlying tempfile.
func (f *FileReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

// NewFileReadSeeker returns FileReadSeeker with reading data from r.
// If you want to reuse it, you must rewind.
func NewFileReadSeeker(r io.Reader) (*FileReadSeeker, error) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(f, r); err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}

	return &FileReadSeeker{
		file: f,
	}, nil
}
