# aws-go-sqs

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/nabeken/aws-go-s3/queue)
[![Build Status](https://img.shields.io/travis/nabeken/aws-go-s3/master.svg)](https://travis-ci.org/nabeken/aws-go-s3)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/nabeken/aws-go-s3/blob/master/LICENSE)

aws-go-s3 is a Amazon S3 library built with [aws/aws-sdk-go](https://github.com/aws/aws-sdk-go).

## Testing

If you want to run the tests, you *SHOULD* use a decicated S3 bucket for the tests.
The test suite issues PutObject and DeleteObject in teardown.

You can specify the bucket name in environment variable.

```sh
$ cd bucket
$ export TEST_S3_BUCKET_NAME=aws-go-s3-test
$ go test -v
```
