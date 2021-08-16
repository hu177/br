package storage

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"io/fs"
	"io/ioutil"
	"os"
	"os/user"
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

//
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
	//TODO:全路径遍历接口
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
	if err != nil {
		return nil, errors.Wrap(err, "HdfsStorage create failure")
	}
	return &HdfsWriter{writer: w}, nil
}

// 转移文件，目录名前都不需要加/
func (s *HdfsStorage) MoveDir(dstDir string, srcDir string) error {
	srcDir =  "/"+srcDir
	files, err := s.client.ReadDir(srcDir)
	if err != nil {
		return errors.Wrap(err, "MoveDir error")
	}
	err = s.client.Mkdir(dstDir, 0644)
	if err != nil {
		return errors.Wrap(err, "MoveDir error")
	}
	for _, v := range files {
		srcFileName := filepath.Join(srcDir, v.Name())
		dstFileName := filepath.Join(dstDir, v.Name())

		err = s.client.Rename(srcFileName, dstFileName)
		if err != nil {
			fmt.Printf("Rename failure:%v",err)
			log.Info("Rename failure" + err.Error())
		}
	}
	return nil
}

type HdfsConfig struct {
	FilePath     string
	CoreSiteConf string
	HdfsSiteConf string
}

type property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []property `xml:"property"`
}

func newHdfsClientWithPath(CoreSitePath string, HdfsPath string) (*hdfs.Client, error) {
	conf, err := loadWithPaths([]string{CoreSitePath, HdfsPath})
	if err != nil {
		return nil, err
	}
	options := hdfs.ClientOptionsFromConf(conf)

	u, err := user.Current()
	if err != nil {
		return nil, err
	}

	options.User = u.Username
	return hdfs.NewClient(options)
}

func loadWithPaths(paths []string) (hadoopconf.HadoopConf, error) {
	fmt.Printf("Loading WithPaths：%v\n", paths)
	var conf hadoopconf.HadoopConf
	for _, file := range paths {
		pList := propertyList{}
		f, err := ioutil.ReadFile(file)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return conf, err
		}

		if conf == nil {
			conf = make(hadoopconf.HadoopConf)
		}

		err = xml.Unmarshal(f, &pList)
		if err != nil {
			return conf, fmt.Errorf("%s: %s", file, err)
		}
		for _, prop := range pList.Property {
			conf[prop.Name] = prop.Value
		}
	}
	return conf, nil
}

func (s *HdfsStorage) dirExist(dirPath string) (bool, error) {
	_, err := s.client.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

func (s *HdfsStorage) GetBaseDir() string {
	return s.base
}
func newHdfsStorage(ctx context.Context, bdh *HdfsConfig, opts *ExternalStorageOptions) (*HdfsStorage, error) {
	// 从命令行配置中读取配置文件
	var retStorage HdfsStorage
	client, err := newHdfsClientWithPath(bdh.CoreSiteConf, bdh.HdfsSiteConf)
	if err != nil {
		return nil, errors.Wrap(err, "newHdfsStorage error")
	}
	retStorage.client = client
	// 先检查该文件夹是否存在，存在需要清除文件夹内容
	base := strings.Join([]string{"/", bdh.FilePath}, "")
	retStorage.base = base
	log.Info("base:" + base)
	if isExt, derr := retStorage.dirExist(base); derr == nil && isExt == true {
		log.Info("dirExist:" + base)
		err := retStorage.client.RemoveAll(base)
		if err != nil {
			return nil, errors.Wrap(err, "newHdfsStorage error")
		}
	} else if derr != nil {
		return nil, errors.Wrap(err, "newHdfsStorage error")
	}
	// 生成文件夹
	err = client.Mkdir(base, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrapf(err, "Create folder :%v error", base)
	}
	return &retStorage, nil
}
