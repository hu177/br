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
	bdh := HdfsConfig{Path: "newbee"}
	ctx := context.Background()
	storage, err := newHdfsStorage(ctx, &bdh, &ExternalStorageOptions{})
	if err != nil {
		fmt.Println(err)
	}
	rw, err := storage.Create(ctx, "hellonew")
	if err != nil {
		fmt.Println(err)
	}
	s := "helloworld"
	_, err = rw.Write(ctx, []byte(s))
	fmt.Println(err)
	err = rw.Close(ctx)
	fmt.Println(err)
}
