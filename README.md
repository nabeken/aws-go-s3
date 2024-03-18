# aws-go-s3

[![PkgGoDev](https://pkg.go.dev/badge/github.com/nabeken/aws-go-s3/v2)](https://pkg.go.dev/github.com/nabeken/aws-go-s3/v2)
![Go](https://github.com/nabeken/aws-go-s3/workflows/Go/badge.svg)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/nabeken/aws-go-s3/blob/master/LICENSE)

`aws-go-s3` is a Amazon S3 utility library built with [aws/aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2).

As of Feb 4, 2024, the master branch is work-in-progress for `aws-sdk-go-v2` support. Please be aware of it.

## v2

Usage:
```go
import "github.com/nabeken/aws-go-s3/v2"
```

### Migration to v2

v2 has the breaking changes noted below.

**Paginator**:

`Bucket#BuildListObjectsV2PaginatorFactory` allows to build `*s3.ListObjectsV2Paginator` through `Bucket.ListObjectsV2PaginatorFactory`. Please refer to [the test code](https://github.com/nabeken/aws-go-s3/blob/master/bucket/bucket_test.go#L164) for the details.

**Presigned Request**:

`Bucket#PresignClient` allows to build a wrapper of `*s3.PresignClient`. Please refer to [the test code](https://github.com/nabeken/aws-go-s3/blob/master/bucket/bucket_test.go#L277) for the details.

## v0

If you want to use this library with `aws-sdk-go`, please use v0 version of the library.

Usage:
```go
import "github.com/nabeken/aws-go-s3"
```

## Testing locally

You can run the test locally with Minio. Each test run will create a new bucket for clean testing.

Launch the minio:
```sh
docker run --rm -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=aws-go-s3 -e MINIO_ROOT_PASSWORD=aws-go-s3 minio/minio server /data --console-address ":9001"
```

then run the test:

```
cd bucket
export AWS_REGION=local
go test -v
```
