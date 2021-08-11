package storage

import (
	"context"

	"github.com/colinmarc/hdfs/v2"
	"github.com/pingcap/errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// TODO: 在csv写入通路中加入该writer
// 实现storage.ExternalFileWriter
type HdfsWriter struct {
	writer *hdfs.FileWriter
}

func (w *HdfsWriter) Write(ctx context.Context, p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *HdfsWriter) Close(ctx context.Context) error {

	if err := w.writer.Flush(); err != nil {
		return errors.Wrap(err, "HdfsWriter close")
	}
	return w.writer.Close()
}

// 实现storage.ExternalFileReader接口
type HdfsReader hdfs.FileReader

// 实现storage.ExternalStorage接口
type HdfsStorage struct {
	base   string
	client *hdfs.Client
}

// WriteFile writes data to a file to storage
func (s *HdfsStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	path := filepath.Join(s.base, name)
	b, err := s.FileExists(ctx, name)
	if err != nil {
		return err
	}
	if b == true {
		// 文件存在，删除重写
		err = s.client.Remove(path)
		return err
	}
	// 文件不存在,创建后写入
	f, err := s.Create(ctx, name)
	if err != nil {
		return err
	}
	_, err = f.Write(ctx, data)
	return err
}

// ReadFile reads the file from the storage and returns the contents
func (s *HdfsStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	path := filepath.Join(s.base, name)
	return s.client.ReadFile(path)
}

func (s *HdfsStorage) FileExists(ctx context.Context, name string) (bool, error) {
	path := filepath.Join(s.base, name)
	_, err := s.client.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

func (s *HdfsStorage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	// TODO:open 路径整理 done
	reader, err := s.client.Open(filepath.Join(s.base, path))
	return reader, errors.Wrap(err, "hdfs open error")
}

// WalkDir traverse all the files in a dir.
func (s *HdfsStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(path string, size int64) error) error {
	path := filepath.Join(s.base, opt.SubDir)
	fileFunction := func(path string, info fs.FileInfo, err error) error {
		return fn(path, info.Size())
	}
	return s.client.Walk(path, fileFunction)
}

func (s *HdfsStorage) URI() string {
	return s.base
}

func (s *HdfsStorage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	name = filepath.Join(s.base, name)
	w, err := s.client.Create(name)
	return &HdfsWriter{writer: w}, errors.Wrap(err, "HdfsStorage create failure")
}

type HdfsConfig struct {
	Path string
}

func newHdfsStorage(ctx context.Context, bdh *HdfsConfig, opts *ExternalStorageOptions) (*HdfsStorage, error) {
	// 从环境中读取配置文件
	client, err := hdfs.New("")
	if err != nil {
		return nil, errors.Wrap(err, "newHdfsStorage error")
	}
	base := strings.Join([]string{"/", bdh.Path}, "")
	// 需要先生成文件夹
	err = client.Mkdir(base, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrapf(err, "Create folder :%v error", base)
	}
	return &HdfsStorage{client: client, base: base}, nil
}
