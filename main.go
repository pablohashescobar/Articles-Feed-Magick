package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"gopkg.in/gographics/imagick.v2/imagick"
)

var awsS3Client *s3.Client

func handleEnvVariables(key string) string {

	if os.Getenv("mode") == "production" {

		viper.BindEnv("AWS_ACCESS_KEY_ID")
		viper.BindEnv("AWS_SECRET_ACCESS_KEY")
		viper.BindEnv("AWS_BUCKET_NAME")
		viper.BindEnv("API_TOKEN")

	} else {
		viper.SetConfigFile(".env")
		// Find and read the config file
		err := viper.ReadInConfig()

		if err != nil {
			log.Fatalf("Error while reading config file %s", err)
		}
	}

	value := viper.GetString(key)

	return value
}

func configS3() {

	creds := credentials.NewStaticCredentialsProvider(handleEnvVariables("AWS_ACCESS_KEY_ID"), handleEnvVariables("AWS_SECRET_ACCESS_KEY"), "")

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(creds), config.WithRegion("ap-south-1"))
	if err != nil {
		log.Printf("error: %v", err)
		return
	}

	awsS3Client = s3.NewFromConfig(cfg)
}

//S3URLtoURI - return map contains bucket name and key
func S3URLtoURI(s3Url string) (map[string]string, error) {
	m := make(map[string]string)
	u, err := url.Parse(s3Url)
	if err != nil {
		return m, err
	}

	if u.Scheme == "s3" {
		//s3: //bucket/key
		m["bucket"] = u.Host
		m["key"] = strings.TrimLeft(u.Path, "/")
	} else if u.Scheme == "https" {
		host := strings.SplitN(u.Host, ".", 2)
		if host[0] == "s3" {
			// No bucket name in the host;
			path := strings.SplitN(u.Path, "/", 3)
			m["bucket"] = path[1]
			m["key"] = path[2]
		} else { //bucket name in host
			m["bucket"] = host[0]
			m["key"] = strings.TrimLeft(u.Path, "/")
		}

	}
	return m, err
}

func DownloadS3File(objectKey string, bucket string, s3Client *s3.Client) ([]byte, error) {

	buffer := manager.NewWriteAtBuffer([]byte{})

	downloader := manager.NewDownloader(s3Client)

	numBytes, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, err
	}

	if numBytes < 1 {
		return nil, errors.New("zero bytes written to memory")
	}

	return buffer.Bytes(), nil
}

func UploadS3File(objectKey string, bucket string, s3Client *s3.Client, fileBytes []byte) error {

	_, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(fileBytes),
	})

	if err != nil {
		return err
	}

	return nil
}

func DeleteS3File(objectKey string, bucket string, s3Client *s3.Client) error {

	_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		return err
	}

	return nil
}

func main() {

	port := ":" + os.Getenv("PORT")

	if port == ":" {
		port = ":8080"
	}

	if os.Getenv("mode") == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	router := gin.Default()

	router.GET("/", Ping)
	router.Use(APITokenMiddleware())
	router.POST("/optimize/", OptimizeImages)

	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"error": "Page not found"})
	})
	router.Run(port)

}

// Respond to errors
func respondWithError(c *gin.Context, code int, message interface{}) {
	c.AbortWithStatusJSON(code, gin.H{"error": message})
}

func APITokenMiddleware() gin.HandlerFunc {

	requiredToken := handleEnvVariables("API_TOKEN")

	// We want to make sure the token is set, bail if not
	if requiredToken == "" {
		log.Fatal("Please set API_TOKEN environment variable")
	}

	return func(c *gin.Context) {

		token := c.Request.Header.Get("token")

		if token == "" {
			respondWithError(c, 401, "API token required")
			return
		}

		if token != requiredToken {
			respondWithError(c, 401, "Invalid API token")
			return
		}

		c.Next()
	}
}

func Ping(c *gin.Context) {
	c.JSON(200, gin.H{"message": "pong"})
}

func OptimizeImages(c *gin.Context) {
	configS3()

	var imageData map[string]interface{}

	if err := c.BindJSON(&imageData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	AWS_S3_URL := imageData["S3_URL"].(string)
	s3map, err := S3URLtoURI(AWS_S3_URL)

	if err != nil {
		respondWithError(c, http.StatusBadRequest, err.Error())
		return
	}

	fileBytes, err := DownloadS3File(s3map["key"], s3map["bucket"], awsS3Client)
	if err != nil {
		respondWithError(c, http.StatusBadRequest, err.Error())
		return
	}

	imagick.Initialize()
	defer imagick.Terminate()

	mw := imagick.NewMagickWand()
	if err != nil {
		respondWithError(c, http.StatusBadRequest, err.Error())
		return
	}

	if err := mw.ReadImageBlob(fileBytes); err != nil {
		respondWithError(c, http.StatusBadRequest, err.Error())
		return
	}

	mw.SetSamplingFactors([]float64{4, 2, 0})
	mw.StripImage()
	mw.SetImageCompressionQuality(80)
	mw.SetImageColorspace(imagick.COLORSPACE_SRGB)

	extension := filepath.Ext(s3map["key"])

	switch extension {
	case ".jpg", ".jpeg":
		mw.SetImageInterlaceScheme(imagick.INTERLACE_JPEG)
	case ".png":
		mw.SetImageInterlaceScheme(imagick.INTERLACE_PNG)
	case ".gif":
		mw.SetImageInterlaceScheme(imagick.INTERLACE_GIF)
	}

	mw.SetImageFormat("webp")

	name := s3map["key"][0 : len(s3map["key"])-len(extension)]

	//Delete the original file
	err = DeleteS3File(s3map["key"], s3map["bucket"], awsS3Client)

	if err != nil {
		respondWithError(c, http.StatusBadRequest, err.Error())
		return
	}

	// Upload the optimized file
	optimizedBucket := handleEnvVariables("AWS_BUCKET_NAME")

	err = UploadS3File(name, optimizedBucket, awsS3Client, mw.GetImageBlob())
	if err != nil {
		respondWithError(c, http.StatusBadRequest, err.Error())
		return
	}
	// Destroy the MagickWand
	mw.Destroy()

	finalUrl := "https://s3.ap-south-1.amazonaws.com/" + optimizedBucket + "/" + name

	c.JSON(http.StatusOK, gin.H{"message": "Image optimized successfully", "url": finalUrl})
}
