package bucket

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nabeken/aws-go-s3/bucket/option"
	"github.com/nabeken/aws-go-s3/ioutils"
	"github.com/stretchr/testify/suite"
)

func testS3Bucket(name string) *Bucket {
	return New(s3.New(session.New()), name)
}

type BucketSuite struct {
	suite.Suite

	bucket *Bucket

	testdata []byte
}

func (s *BucketSuite) SetupSuite() {
	name := os.Getenv("TEST_S3_BUCKET_NAME")
	if len(name) == 0 {
		s.T().Skip("TEST_S3_BUCKET_NAME must be set")
	}

	s.bucket = testS3Bucket(name)

	data, err := json.Marshal(struct{ Time time.Time }{Time: time.Now()})
	if !s.NoError(err) {
		s.T().Fatal(err)
	}

	s.testdata = data
}

func (s *BucketSuite) TestObject() {
	key := "test-object"
	ct := "application/json"
	cl := int64(len(s.testdata))

	content, err := ioutils.NewFileReadSeeker(bytes.NewReader(s.testdata))
	if !s.NoError(err) {
		return
	}
	defer content.Close()

	// 1. Put new object
	{
		_, err := s.bucket.PutObject(
			key,
			content,
			option.ContentType(ct),
			option.ContentLength(cl),
			option.ACLPrivate(),
		)

		if !s.NoError(err) {
			return
		}
	}

	// 2. Get the object and assert its metadata and content
	{
		object, err := s.bucket.GetObject(key)
		if !s.NoError(err) {
			return
		}

		body, err := ioutil.ReadAll(object.Body)
		if !s.NoError(err) {
			return
		}
		defer object.Body.Close()

		s.Equal(ct, *object.ContentType)
		s.Equal(cl, *object.ContentLength)
		s.Equal(s.testdata, body)
	}

	// 3. The object must exist
	{
		exists, err := s.bucket.ExistsObject(key)
		s.NoError(err)
		s.True(exists)
	}

	// 4. Delete the object
	{
		_, err := s.bucket.DeleteObject(key)
		if !s.NoError(err) {
			return
		}
	}

	// 5. Head the object
	{
		_, err := s.bucket.HeadObject(key)
		s.Error(err)
	}

	// 6. The object must not exist
	{
		exists, err := s.bucket.ExistsObject(key)
		s.NoError(err)
		s.False(exists)
	}
}

func TestBucketSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test")
	}

	suite.Run(t, new(BucketSuite))
}
