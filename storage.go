package go_google_storage

import (
	"bytes"
	"compress/gzip"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
	"io/ioutil"
	"log"
	// "net/http"
	"time"
)

type StorageClient struct {
	Client *storage.Service
}

func (c *StorageClient) Store(bucketName, fileName string, content []byte) string {

	object := &storage.Object{Name: fileName}

	var b bytes.Buffer
	w := gzip.NewWriter(&b)

	w.Write(content)

	w.Close()
	file := bytes.NewReader(b.Bytes())

	res, err := c.Client.Objects.Insert(bucketName, object).Media(file).Do()
	if err != nil {
		log.Printf("Objects.Insert failed: %v\n", err)
		time.Sleep(10 * time.Second)
		return c.Store(bucketName, fileName, content)
	}

	return res.SelfLink
}

func NewStorageClient(keyFile string) *StorageClient {
	c := new(StorageClient)

	data, err := ioutil.ReadFile(keyFile)
	if err != nil {
		log.Fatal(err)
	}

	conf, err := google.JWTConfigFromJSON(data, []string{storage.CloudPlatformScope}...)
	if err != nil {
		log.Fatal(err)
	}

	client := conf.Client(oauth2.NoContext)

	service, err := storage.New(client)
	if err != nil {
		log.Fatalf("Unable to create storage service: %v", err)
	}
	c.Client = service

	return c
}
