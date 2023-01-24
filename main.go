package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const ACCESS_KEY = ""
const SECRET_KEY = ""
const ACCOUNT_ID = ""
const SAMPLE_FILE = "sample.mov"

type S3WriteCloser struct {
	Part        int
	WriteCloser io.WriteCloser
}

func main() {
	file, err := os.Open(SAMPLE_FILE)
	if err != nil {
		fmt.Println("Failed to open file", err)
		return
	}
	defer file.Close()

	uploader := getUploader(ACCOUNT_ID, ACCESS_KEY, SECRET_KEY)
	closer := writer(uploader)
	buffer := make([]byte, 1024*5)

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
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Wrote %d bytes\n", n)
	}

	closer.WriteCloser.Close()
	time.Sleep(10 * time.Second)
}

// writer will be invoked for each part of the file that we want
// to upload in parallel
func writer(uploader *s3manager.Uploader) *S3WriteCloser {
	r, w := io.Pipe()
	writeCloser := S3WriteCloser{
		Part:        1,
		WriteCloser: w,
	}

	go func(uploader *s3manager.Uploader, reader *io.PipeReader) {
		err := processUpload(uploader, r)
		if err != nil {
			log.Fatal("error uploading to R2", err)
		}
		fmt.Printf("Uploaded part: %d\n", writeCloser.Part)
	}(uploader, r)

	return &writeCloser
}

// processUpload will handle chunks of multipart uploads and will complete
// once the io writer is closed
func processUpload(
	uploader *s3manager.Uploader,
	reader *io.PipeReader,
) error {
	fmt.Println("Uploading to AWS")
	uploadOptions := s3manager.UploadInput{
		Bucket: aws.String("test-multipart"),
		Key:    aws.String("sample4.mov"),
		Body:   aws.ReadSeekCloser(reader),
	}

	_, err := uploader.Upload(&uploadOptions)
	if err != nil {
		fmt.Println("Failed to upload file", err)
		return err
	}
	return nil
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
