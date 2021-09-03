package storage

import (
	"context"
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	krb "github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"io/fs"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
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
	base   string // 指的是tmp目录
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
		if err != nil {
			return errors.Wrapf(err, "remove file %v", path)
		}
	}
	// 文件不存在,创建后写入
	f, err := s.Create(ctx, name)
	if err != nil {
		return err
	}
	_, err = f.Write(ctx, data)
	return errors.Wrap(err, "Hdfs writeFile")
}

// ReadFile reads the file from the storage and returns the contents
func (s *HdfsStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	path := filepath.Join(s.base, name)
	return s.client.ReadFile(path)
}

// 返回true代表存在，返回false代表不存在
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
	// 判断是否存在
	exist, err := s.FileExists(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(err, "Find file %v exist error", name)
	}
	cname := filepath.Join(s.base, name)
	if exist { // 存在直接删除
		err = s.client.Remove(cname)
		if err != nil {
			return nil, errors.Wrapf(err, "Remove file %v error", name)
		}
	}
	w, err := s.client.Create(cname)
	if err != nil {
		return nil, errors.Wrap(err, "HdfsStorage create failure")
	}
	return &HdfsWriter{writer: w}, nil
}

// 从提供目录的.dumptmp文件夹下转移文件到外部，并删除dumptmp目录
func (s *HdfsStorage) MoveDir(dstDir string, srcDir string) error {
	files, err := s.client.ReadDir(srcDir)
	if err != nil {
		return errors.Wrapf(err, "ReadDir %v error", srcDir)
	}

	for _, v := range files {
		srcFileName := filepath.Join(srcDir, v.Name())
		dstFileName := filepath.Join(dstDir, v.Name())

		err = s.client.Rename(srcFileName, dstFileName)
		if err != nil {
			fmt.Printf("Rename failure:%v", err)
		}
	}

	return s.client.Remove(srcDir)
}

type HdfsConfig struct {
	FilePath     string
	CoreSiteConf string
	HdfsSiteConf string
}

func getKerberosClient() (*krb.Client, error) {
	configPath := os.Getenv("KRB5_CONFIG")
	if configPath == "" {
		configPath = "/etc/krb5.conf"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}

	// Determine the ccache location from the environment, falling back to the
	// default location.
	ccachePath := os.Getenv("KRB5CCNAME")
	if strings.Contains(ccachePath, ":") {
		if strings.HasPrefix(ccachePath, "FILE:") {
			ccachePath = strings.SplitN(ccachePath, ":", 2)[1]
		} else {
			return nil, fmt.Errorf("unusable ccache: %s", ccachePath)
		}
	} else if ccachePath == "" {
		u, err := user.Current()
		if err != nil {
			return nil, err
		}

		ccachePath = fmt.Sprintf("/tmp/krb5cc_%s", u.Uid)
	}

	ccache, err := credentials.LoadCCache(ccachePath)
	if err != nil {
		return nil, err
	}

	client, err := krb.NewFromCCache(ccache, cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
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

// 返回tmp目录地址
func (s *HdfsStorage) GetBaseDir() string {
	return s.base
}

func newHdfsStorage(ctx context.Context, bdh *HdfsConfig, opts *ExternalStorageOptions) (*HdfsStorage, error) {
	// 从命令行配置中读取配置文件
	//client, err := newHdfsClientWithPath()
	client, err := NewHdfsClient()
	if err != nil {
		return nil, errors.Wrap(err, "newHdfsStorage error")
	}

	retStorage := &HdfsStorage{client: client}
	retStorage.client = client
	// 先检查该文件夹是否存在，存在需要清除文件夹内容
	base := bdh.FilePath

	isExt, err := retStorage.dirExist(base)
	if err != nil {
		return nil, errors.Wrap(err, "newHdfsStorage error")
	}
	if isExt {
		log.Info("the dir has exist:" + base + " , it will be deleted")
		// 遍历删除所有文件夹中的内容
		files, err := retStorage.client.ReadDir(base)
		if err != nil {
			return nil, errors.Wrap(err, "newHdfsStorage error")
		}
		for _, v := range files {
			filename := filepath.Join(base, v.Name())
			err := retStorage.client.RemoveAll(filename)
			if err != nil {
				return nil, errors.Wrapf(err, "remove file %v error", filename)
			}
		}
	}

	retStorage.base = filepath.Join(base, ".dumptmp")
	// 生成tmp文件夹
	err = client.MkdirAll(retStorage.base, os.FileMode(0o644))
	if err != nil {
		return nil, errors.Wrapf(err, "Create folder :%v error", base+".dumptmp")
	}
	return retStorage, nil
}

// 进行HDFS的重连,重连错误，直接返回原来的链接
func (s *HdfsStorage) ReConnect() (*HdfsStorage, error) {
	var retStorage HdfsStorage
	retStorage.base = s.base
	client, err := NewHdfsClient()
	if err != nil {
		return s, errors.Wrap(err, "Create client failure")
	}
	retStorage.client = client
	return &retStorage, nil
}

// 获取新的Client链接
func NewHdfsClient() (*hdfs.Client, error) {
	namenode := os.Getenv("HADOOP_NAMENODE")
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("Problem loading configuration: %s", err)
	}
	options := hdfs.ClientOptionsFromConf(conf)
	if namenode != "" {
		options.Addresses = []string{namenode}
	}

	if options.Addresses == nil {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths. Alternatively, set HADOOP_NAMENODE or HADOOP_CONF_DIR in your environment.")
	}

	if options.KerberosClient != nil {
		options.KerberosClient, err = getKerberosClient()
		if err != nil {
			return nil, fmt.Errorf("Problem with kerberos authentication: %s", err)
		}
	} else {
		options.User = os.Getenv("HADOOP_USER_NAME")
		if options.User == "" {
			u, err := user.Current()
			if err != nil {
				return nil, fmt.Errorf("Couldn't determine user: %s", err)
			}

			options.User = u.Username
		}
	}

	dialFunc := (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 5 * time.Second,
		DualStack: true,
	}).DialContext

	options.NamenodeDialFunc = dialFunc
	options.DatanodeDialFunc = dialFunc

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("Couldn't connect to namenode: %s", err)
	}
	return c, nil
}
