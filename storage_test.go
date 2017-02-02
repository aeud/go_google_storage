package go_google_storage

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
)

func TestStore(t *testing.T) {
	log.Println("Start")
	bs, _ := json.Marshal(struct {
		Foo string `json:"foo"`
	}{"bar"})
	NewStorageClient("/Users/adrien/.ssh/google.json").Store("lx-ga", "test.json.gz", bs)
}

func TestInsertBucket(t *testing.T) {
	log.Println("Start")
	c := NewStorageClient("/Users/adrien/.ssh/google.json")
	c.CreateBucket("luxola.com:luxola-analytics", "lx-new-test-4378297")
}

func TestDeleteObjects(t *testing.T) {
	log.Println("Start")
	c := NewStorageClient("/Users/adrien/.ssh/google.json")
	c.EmptyBucket("lx-test")
}

func TestDeleteBucket(t *testing.T) {
	log.Println("Start")
	c := NewStorageClient("/Users/adrien/.ssh/google.json")
	c.DeleteBucket("lx-new-test-4378297")
}

func TestInsertBucketIfNotExists(t *testing.T) {
	log.Println("Start")
	c := NewStorageClient("/Users/ae/.ssh/google.json")
	exists, err := c.CreateBucketIfNotExists("luxola.com:luxola-analytics", "lx-test-mssql-daily-export")
	fmt.Println(exists, err)
}
