package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const ACCESS_KEY = ""
const SECRET_KEY = ""
const ACCOUNT_ID = ""
const SAMPLE_FILE = ""
const BUCKET_NAME = ""

type S3WriteCloser struct {
	Part        int
	WriteCloser io.WriteCloser
}

type S3Metadata struct {
	BucketName    string
	Key           string
	ContentLength int
	wg            *sync.WaitGroup
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	file, err := os.Open(SAMPLE_FILE)
	if err != nil {
		fmt.Println("Failed to open file", err)
		return
	}

	// only necesary for PutObject requests that need content-length header
	stat, err := file.Stat()
	if err != nil {
		fmt.Println("Failed to open file", err)
		return
	}

	uploader := getUploader(ACCOUNT_ID, ACCESS_KEY, SECRET_KEY)
	closer := writer(uploader, S3Metadata{
		BucketName:    BUCKET_NAME,
		Key:           SAMPLE_FILE,
		ContentLength: int(stat.Size()),
		wg:            &wg,
	})

	// simulate range requests or chunking here
	buffer := make([]byte, 1024*1024)
	for {
		_, err = file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading file:", err)
			return
		}

		n, err := closer.WriteCloser.Write(buffer)
		closer.Part++
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Wrote %d mb\n", n/1024)
	}

	closer.WriteCloser.Close()
	fmt.Println("Waiting for Upload to complete...")
	wg.Wait()
	fmt.Println("Upload Completed")
}

// writer will be invoked for each part of the file that we want
// to upload in parallel
func writer(uploader *s3manager.Uploader, metadata S3Metadata) *S3WriteCloser {
	r, w := io.Pipe()
	writeCloser := S3WriteCloser{
		Part:        1,
		WriteCloser: w,
	}

	go func(uploader *s3manager.Uploader, reader *io.PipeReader, metadata S3Metadata) {
		err := processUpload(uploader, r, metadata)
		if err != nil {
			log.Fatal("error uploading to R2", err)
		}
		fmt.Printf("Uploaded a total of %d parts\n", writeCloser.Part)
	}(uploader, r, metadata)
	return &writeCloser
}

// processUpload will handle chunks of multipart uploads and will complete
// once the io writer is closed
func processUpload(
	uploader *s3manager.Uploader,
	reader *io.PipeReader,
	metadata S3Metadata,
) error {
	fmt.Println("Uploading to AWS")
	upload := func() error {
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(metadata.BucketName),
			Key:    aws.String(metadata.Key),
			Body:   aws.ReadSeekCloser(reader),
		})
		if err == nil {
			metadata.wg.Done()
		}
		return err
	}

	// TODO: Add HEAD check to check existence of bucket instead of relying on failure
	if err := handleBucketCreation(uploader, metadata.BucketName); err != nil {
		fmt.Println(err)
		return err
	}
	if err := upload(); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func handleBucketCreation(uploader *s3manager.Uploader, bucket string) error {
	createBucketInput := &s3.CreateBucketInput{
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String("hint:WNAM"),
		},
		Bucket: aws.String(bucket),
	}
	_, err := uploader.S3.CreateBucket(createBucketInput)
	return err
}

// getUploader returns uploader instance to interface with R2
func getUploader(accountId, accessKey, secretKey string) *s3manager.Uploader {
	endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Endpoint:    &endpoint,
	})
	if err != nil {
		log.Fatal(err)
	}
	return s3manager.NewUploader(sess)
}
