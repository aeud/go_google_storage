package go_google_storage

import (
	"bytes"
	"compress/gzip"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
	"io/ioutil"
	"log"
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

func (c *StorageClient) CreateBucket(projectId, bucketName string) {
	bs := storage.NewBucketsService(c.Client)
	bucket := new(storage.Bucket)
	bucket.Name = bucketName
	_, err := bs.Insert(projectId, bucket).Do()
	if err != nil {
		log.Fatalf("Error when creating: %v\n", err)
	}
}

func (c *StorageClient) GetObjectsAndExecute(bucketName string, f func([]*storage.Object)) {
	os := storage.NewObjectsService(c.Client).List(bucketName)
	os.MaxResults(105)
	ExtractObjects(os, "", f)
}

func ExtractObjects(os *storage.ObjectsListCall, nextPageToken string, f func([]*storage.Object)) {
	os.PageToken(nextPageToken)
	objects, err := os.Do()
	if err != nil {
		log.Fatalln(err)
	}
	items := objects.Items
	f(items)
	if objects.NextPageToken != "" {
		ExtractObjects(os, objects.NextPageToken, f)
	}
}

func (c *StorageClient) DeleteObject(object *storage.Object) {
	// log.Printf("Deleting %v\n", object.Name)
	err := storage.NewObjectsService(c.Client).Delete(object.Bucket, object.Name).Do()
	if err != nil {
		log.Fatalln(err)
	}
}

func (c *StorageClient) EmptyBucket(bucketName string) {
	c.GetObjectsAndExecute(bucketName, func(is []*storage.Object) {
		for _, o := range is {
			c.DeleteObject(o)
		}
	})
}

func (c *StorageClient) DeleteBucket(bucketName string) {
	c.EmptyBucket(bucketName)
	time.Sleep(10 * time.Second)
	bs := storage.NewBucketsService(c.Client)
	err := bs.Delete(bucketName).Do()
	if err != nil {
		log.Fatalln(err)
	}
}
