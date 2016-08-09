package go_google_storage

import (
	"encoding/json"
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
