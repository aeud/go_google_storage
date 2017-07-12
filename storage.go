package go_google_storage

import (
	"compress/gzip"
	"io/ioutil"
	"log"
	"time"

	"io"

	"bytes"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
)

// StorageClient is the main type
type StorageClient struct {
	Client *storage.Service
}

// Store upload a file to Google Storage
func (c *StorageClient) Store(bucketName, fileName string, content []byte) string {

	object := &storage.Object{Name: fileName}
	r2 := bytes.NewReader(content)

	pr, pw := io.Pipe()
	defer pr.Close()

	// var b bytes.Buffer
	w := gzip.NewWriter(pw)

	go func() {
		defer w.Close()
		defer pw.Close()
		io.Copy(w, r2)
	}()

	// w.Close()
	// pr.Close()
	// pw.Close()
	// file := bytes.NewReader(b.Bytes())

	res, err := c.Client.Objects.Insert(bucketName, object).Media(pr).Do()
	if err != nil {
		log.Printf("Objects.Insert failed, retrying: %v\n", err)
		time.Sleep(10 * time.Second)
		return c.Store(bucketName, fileName, content)
	}

	return res.SelfLink
}

// NewStorageClient generates a new storage client
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

// CreateBucket creates an emptuy bucket
func (c *StorageClient) CreateBucket(projectId, bucketName string) {
	bs := storage.NewBucketsService(c.Client)
	bucket := new(storage.Bucket)
	bucket.Name = bucketName
	_, err := bs.Insert(projectId, bucket).Do()
	if err != nil {
		log.Fatalf("Error when creating: %v\n", err)
	}
}

// CreateBucketIfNotExists creates an emptuy bucket if not existing already, return exists and error
func (c *StorageClient) CreateBucketIfNotExists(projectID, bucketName string) (bool, error) {
	var err error
	bs := storage.NewBucketsService(c.Client)
	_, err = bs.Get(bucketName).Do()
	if err == nil {
		return true, nil
	}
	bucket := new(storage.Bucket)
	bucket.Name = bucketName
	_, err = bs.Insert(projectID, bucket).Do()
	if err != nil {
		return false, err
	}
	return false, nil
}

// GetObjectsAndExecute executes a lambda function to all elements
func (c *StorageClient) GetObjectsAndExecute(bucketName string, f func([]*storage.Object)) {
	os := storage.NewObjectsService(c.Client).List(bucketName)
	os.MaxResults(105)
	ExtractObjects(os, "", f)
}

// ExtractObjects gets all the objects from a bucket
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

// DeleteObject deletes a single object from Google Storage
func (c *StorageClient) DeleteObject(object *storage.Object, try int) (err error) {
	// log.Printf("Deleting %v\n", object.Name)
	err = storage.NewObjectsService(c.Client).Delete(object.Bucket, object.Name).Do()
	if err != nil && try < 5 {
		log.Printf("Error when deleting object, retrying: %v, %v\n", object.Id, err)
		return c.DeleteObject(object, try+1)
	}
	return
}

// EmptyBucket removes all the files from a bucket
func (c *StorageClient) EmptyBucket(bucketName string) (e error) {
	c.GetObjectsAndExecute(bucketName, func(is []*storage.Object) {
		for _, o := range is {
			if err := c.DeleteObject(o, 0); err != nil {
				log.Printf("Error when deleting object: %v, %v\n", o.Id, err)
				e = err
			}
		}
	})
	return
}

// DeleteBucket removes a bucket from Google Storage
func (c *StorageClient) DeleteBucket(bucketName string) error {
	if err := c.EmptyBucket(bucketName); err != nil {
		return err
	}
	bs := storage.NewBucketsService(c.Client)
	if err := bs.Delete(bucketName).Do(); err != nil {
		log.Printf("Retrying deleting bucket in 5 sec: %v\n", err)
		time.Sleep(5 * time.Second)
		return c.DeleteBucket(bucketName)
	}
	return nil
}
