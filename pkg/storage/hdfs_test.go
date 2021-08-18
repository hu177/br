package storage

import (
	"context"
	"fmt"
	"testing"
)

func TestWriteFile(t *testing.T) {
	bdh := HdfsConfig{}
	storage, err := newHdfsStorage(context.TODO(), &bdh, &ExternalStorageOptions{})
	if err != nil {
		fmt.Println(err)
	}
	data := "helloworld"
	err = storage.WriteFile(context.TODO(), "/pikaqiu", []byte(data))
	fmt.Println(err)
}

func TestWrite(t *testing.T) {
}
