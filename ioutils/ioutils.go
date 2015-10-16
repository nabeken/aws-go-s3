// Package ioutils provides wrappers to fill the gap between io.Reader and io.ReadSeeker.
// e.g. io.ReadCloser in http.Request.Body and io.ReadSeeker in s3.PutObjectInput.Body.
package ioutils

import (
	"io"
	"io/ioutil"
	"os"
)

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
