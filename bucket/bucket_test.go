package bucket_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/nabeken/aws-go-s3/v2/bucket"
	"github.com/nabeken/aws-go-s3/v2/bucket/option"
	"github.com/nabeken/aws-go-s3/v2/ioutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNewTestData() []byte {
	data, err := json.Marshal(struct{ Time time.Time }{Time: time.Now()})
	if err != nil {
		panic(err)
	}

	return data
}

func TestBucket(t *testing.T) {
	b := newTestBucket(t)

	origData := mustNewTestData()
	origKey := "test-object"
	ct := "application/json"
	cl := int64(len(origData))

	t.Run("Put a new object", func(t *testing.T) {
		content, err := ioutils.NewFileReadSeeker(bytes.NewReader(origData))
		require.NoError(t, err)

		defer content.Close()

		_, err = b.PutObject(
			context.TODO(),
			origKey,
			content,
			option.ContentType(ct),
			option.ContentLength(cl),
			option.ACLPrivate(),
		)

		require.NoError(t, err)
	})

	t.Run("Copy the object", func(t *testing.T) {
		_, err := b.CopyObject(context.TODO(), "copy-"+origKey, origKey)
		require.NoError(t, err)
	})

	t.Run("Assert the objects", func(t *testing.T) {
		for _, key := range []string{origKey, "copy-" + origKey} {
			t.Run(fmt.Sprintf("GetObject(%s)", key), func(t *testing.T) {
				object, err := b.GetObject(context.TODO(), key)
				require.NoError(t, err)

				defer object.Body.Close()

				body, err := io.ReadAll(object.Body)
				require.NoError(t, err)

				assert.Equal(t, ct, *object.ContentType)
				assert.Equal(t, cl, *object.ContentLength)
				assert.Equal(t, origData, body)
			})

			t.Run(fmt.Sprintf("ExistsObject(%s)", key), func(t *testing.T) {
				exists, err := b.ExistsObject(context.TODO(), key)
				require.NoError(t, err)
				assert.True(t, exists)
			})

			t.Run(fmt.Sprintf("DeleteObject(%s)", key), func(t *testing.T) {
				_, err := b.DeleteObject(context.TODO(), key)
				require.NoError(t, err)
			})

			t.Run(fmt.Sprintf("HeadObject(%s)", key), func(t *testing.T) {
				_, err := b.HeadObject(context.TODO(), key)
				assert.Error(t, err)

				var ae smithy.APIError
				assert.ErrorAs(t, err, &ae)
				assert.Equal(t, "NotFound", ae.ErrorCode())
			})

			t.Run(fmt.Sprintf("ExistsObject(%s) after DeleteObject", key), func(t *testing.T) {
				exists, err := b.ExistsObject(context.TODO(), key)
				require.NoError(t, err)
				assert.False(t, exists)
			})
		}
	})

	t.Run("Assert DeleteObjects", func(t *testing.T) {
		t.Run("Create 1000 objects", func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				br, err := ioutils.NewFileReadSeeker(bytes.NewReader(origData))
				require.NoError(t, err)

				key := fmt.Sprintf("prefix/key-%d.json", i)
				_, err = b.PutObject(
					context.TODO(),
					key,
					br,
					option.ContentType(ct),
					option.ContentLength(cl),
					option.ACLPrivate(),
				)

				require.NoError(t, err)
			}
		})

		t.Run("Assert the objects", func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("prefix/key-%d.json", i)
				exists, err := b.ExistsObject(context.TODO(), key)
				require.NoError(t, err)
				require.True(t, exists)
			}
		})

		t.Run("Delete the objects", func(t *testing.T) {
			var identifiers []types.ObjectIdentifier

			for i := 0; i < 1000; i++ {
				identifiers = append(identifiers, types.ObjectIdentifier{
					Key: aws.String(fmt.Sprintf("prefix/key-%d.json", i)),
				})
			}

			_, err := b.DeleteObjects(context.TODO(), identifiers)
			require.NoError(t, err)
		})

		t.Run("Assert the objects deleted", func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("prefix/key-%d.json", i)
				exists, err := b.ExistsObject(context.TODO(), key)
				require.NoError(t, err)
				require.False(t, exists)
			}
		})
	})
}

func TestBucketPaginator(t *testing.T) {
	b := newTestBucket(t)

	// enable versionings
	_, err := b.S3.PutBucketVersioning(
		context.TODO(),
		&s3.PutBucketVersioningInput{
			Bucket: b.Name,
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		},
	)

	require.NoError(t, err)

	origData := mustNewTestData()
	ct := "application/json"
	cl := int64(len(origData))
	prefix := "prefix-paginator-test"

	var keys []string

	for i := 0; i < 2000; i++ {
		keys = append(keys, fmt.Sprintf("%s/key-%d.json", prefix, i))
	}

	t.Run("Put 2000 objects twice for the paginator testing", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			for _, key := range keys {
				_, err := b.PutObject(
					context.TODO(),
					key,
					bytes.NewReader(origData),
					option.ContentType(ct),
					option.ContentLength(cl),
					option.ACLPrivate(),
				)

				require.NoError(t, err)
			}
		}
	})

	t.Run("List every single object", func(t *testing.T) {
		var cnt int
		paginator := b.BuildListObjectsV2PaginatorFactory(
			context.TODO(),
			prefix+"/",
			func(req *s3.ListObjectsV2Input) {
				req.MaxKeys = aws.Int32(1)
			},
		)(func(req *s3.ListObjectsV2PaginatorOptions) {
			req.Limit = 1
		})

		var actualKeys []string

		for paginator.HasMorePages() {
			cnt++

			output, err := paginator.NextPage(context.TODO())
			require.NoError(t, err)

			for _, obj := range output.Contents {
				actualKeys = append(actualKeys, *obj.Key)
			}

			// t.Logf("Debug paginator:%d / %d /  %d\n", cnt, len(output.Contents), len(actualKeys))
		}

		// the last call contains an empty list that indicates the paginator is at the last page
		assert.Equal(t, len(keys)+1, cnt)
		assert.ElementsMatch(t, keys, actualKeys)
	})

	t.Run("List every versioning object", func(t *testing.T) {
		var cnt int
		paginator := b.BuildListObjectVersionsPaginatorFactory(
			context.TODO(),
			prefix+"/",
			func(req *s3.ListObjectVersionsInput) {
				req.MaxKeys = aws.Int32(1)
			},
		)(func(req *s3.ListObjectVersionsPaginatorOptions) {
			req.Limit = 1
		})

		var actualKeys []string

		for paginator.HasMorePages() {
			cnt++

			output, err := paginator.NextPage(context.TODO())
			require.NoError(t, err)

			for _, obj := range output.Versions {
				actualKeys = append(actualKeys, *obj.Key)
			}

			// t.Logf("Debug paginator:%d / %d /  %d\n", cnt, len(output.Versions), len(actualKeys))
		}

		// MinIO seems to return the last page at the 4000th call
		assert.Equal(t, len(keys)*2, cnt)

		versioningKeys := append([]string{}, keys...)
		versioningKeys = append(versioningKeys, keys...)

		assert.ElementsMatch(t, versioningKeys, actualKeys)
	})
}

func TestPresignClient(t *testing.T) {
	b := newTestBucket(t)

	pc := b.PresignClient(func(opts *s3.PresignOptions) {
		opts.Expires = time.Minute
	})

	origData := mustNewTestData()
	ct := "application/json"
	cl := int64(len(origData))
	key := "presigned-object"

	t.Run("PresignPutObject", func(t *testing.T) {
		signedReq, err := pc.PresignPutObject(
			context.TODO(),
			key,
			bytes.NewReader(origData),
			option.ContentType(ct),
			option.ContentLength(cl),
			option.ACLPrivate(),
		)

		require.NoError(t, err)

		resp, err := doPresignedRequest(signedReq, bytes.NewReader(origData))
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, 200, resp.StatusCode)
	})

	t.Run("PresignGetObject", func(t *testing.T) {
		signedReq, err := pc.PresignGetObject(
			context.TODO(),
			"presigned-object",
		)

		require.NoError(t, err)

		resp, err := doPresignedRequest(signedReq, nil)
		require.NoError(t, err)

		defer resp.Body.Close()

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, 200, resp.StatusCode)
		assert.Equal(t, origData, b)
	})

	t.Run("PresignHeadObject", func(t *testing.T) {
		signedReq, err := pc.PresignHeadObject(
			context.TODO(),
			"presigned-object",
		)

		require.NoError(t, err)

		resp, err := doPresignedRequest(signedReq, nil)
		require.NoError(t, err)

		defer resp.Body.Close()

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, 200, resp.StatusCode)
		assert.Empty(t, b)
	})

	t.Run("PresignDeleteObject", func(t *testing.T) {
		signedReq, err := pc.PresignDeleteObject(
			context.TODO(),
			"presigned-object",
		)

		require.NoError(t, err)

		resp, err := doPresignedRequest(signedReq, nil)
		require.NoError(t, err)

		defer resp.Body.Close()

		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, 204, resp.StatusCode)
		assert.Empty(t, b)

		t.Run("NotFound after DeleteObject", func(t *testing.T) {
			signedReq, err := pc.PresignGetObject(
				context.TODO(),
				"presigned-object",
			)

			require.NoError(t, err)

			resp, err := doPresignedRequest(signedReq, nil)
			require.NoError(t, err)

			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, 404, resp.StatusCode)
			assert.Contains(t, string(b), "NoSuchKey")
		})
	})
}

func doPresignedRequest(signedReq *v4.PresignedHTTPRequest, r io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(signedReq.Method, signedReq.URL, r)
	if err != nil {
		return nil, err
	}

	// replace with the signed headers
	req.Header = signedReq.SignedHeader

	return http.DefaultClient.Do(req)
}

func newTestBucket(t *testing.T) *bucket.Bucket {
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion != "local" {
		t.Skip("AWS_REGION must be set to local")
	}

	bucketName := fmt.Sprintf("aws-go-s3-testing-%d", time.Now().UnixNano())

	s3c := newMinioS3Client()
	ctx := context.TODO()

	_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	require.NoError(t, err)

	return bucket.New(s3c, bucketName)
}

func newMinioS3Client() *s3.Client {
	return s3.New(s3.Options{
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("aws-go-s3", "aws-go-s3", "")),
		Region:       "local",
		BaseEndpoint: aws.String("http://127.0.0.1:9000"),
	})
}
