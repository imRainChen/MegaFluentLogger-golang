package fluentd

import (
	"time"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"github.com/nsqio/go-diskqueue"
	"fmt"
)

const (
	defaultHost                   = "127.0.0.1"
	defaultNetwork                = "tcp"
	defaultSocketPath             = ""
	defaultPort                   = 24224
	defaultTimeout                = 3 * time.Second
	defaultWriteTimeout           = 3 * time.Second // time.Duration(0) will not time out
	defaultReadTimeout            = 3 * time.Second
	defaultRetryWait              = 500
	defaultMaxRetry               = 0
	defaultReconnectWaitIncreRate = 1.5
	defaultMaxBytesPerBuffer      = 2 * 1024 * 1024
)

type Config struct {
	FluentPort       int
	FluentHost       string
	FluentNetwork    string
	FluentSocketPath string
	Timeout          time.Duration		// 连接超时
	WriteTimeout     time.Duration		// 写入数据超时
	ReadTimeout		 time.Duration		// 读取数据超时
	RetryWait        int				// 重连等待时间
	MaxRetry         int				// 最大重连次数，0为无限
	TagPrefix        string				// fluentd Tag前缀
	MaxBytesPerBuffer   int				// 每块缓冲区大小，超过该缓冲区则写入文件
	BufferDataPath   string				// 缓冲数据存储路径
}

type Option struct {
	Chunk string	`codec:"chunk"`
}

type Forward struct {
	_struct bool     `codec:",toarray"`
	Tag string
	Entries []byte	 `codec:"entries"`
	Option			 `codec:"option"`
}

// Record的结构配置参考 http://ugorji.net/blog/go-codec-primer
type Entry struct{
	_struct bool     `codec:",toarray"`
	Time int64
	Record interface{} `codec:"record"`
}

type fluentd struct {
	connections map[string]*tagConn
	config Config
	waitGroup sync.WaitGroup
	logf diskqueue.AppLogFunc
}

func New(config Config, logf diskqueue.AppLogFunc) (*fluentd, error) {
	if config.FluentNetwork == "" {
		config.FluentNetwork = defaultNetwork
	}

	if config.FluentHost == "" {
		config.FluentHost = defaultHost
	}

	if config.FluentPort == 0 {
		config.FluentPort = defaultPort
	}

	if config.FluentSocketPath == "" {
		config.FluentSocketPath = defaultSocketPath
	}

	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}

	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaultWriteTimeout
	}

	if config.ReadTimeout == 0 {
		config.ReadTimeout = defaultReadTimeout
	}

	if config.MaxBytesPerBuffer == 0 {
		config.MaxBytesPerBuffer = defaultMaxBytesPerBuffer
	}

	if config.RetryWait == 0 {
		config.RetryWait = defaultRetryWait
	}

	if config.MaxRetry == 0 {
		config.MaxRetry = defaultMaxRetry
	}

	if logf == nil {
		logf = NewLogger()
	}

	fileInfo, err := os.Stat(config.BufferDataPath)
	if err != nil {
		return nil, err
	}

	if !fileInfo.IsDir() {
		return nil, fmt.Errorf("BufferDataPath:%s not is dir", config.BufferDataPath)
	}

	f := fluentd{
		config: config,
		connections: make(map[string]*tagConn),
		logf: logf,
	}

	err = filepath.Walk(config.BufferDataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		suffix := ".diskqueue.meta.dat"
		if strings.HasSuffix(info.Name(), suffix) {
			tag := strings.TrimSuffix(info.Name(), suffix)
			conn, err := NewTagConn(tag, f.config, logf)
			if err != nil {
				return err
			}
			f.connections[tag] = conn
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &f, nil
}

func (f *fluentd) Push(tag string, data interface{}) error {
	entry := Entry{Time: time.Now().Unix(), Record: data}
	if err := f.pushEntry(tag, entry); err != nil {
		return err
	}

	return nil
}

func (f *fluentd) PushWithTime(tag string, timeUnix int64, data interface{}) error  {
	entry := Entry{Time: timeUnix, Record: data}
	if err := f.pushEntry(tag, entry); err != nil {
		return err
	}

	return nil
}

func (f *fluentd) pushEntry(tag string, entry Entry) (err error) {
	conn, ok := f.connections[tag]
	if !ok {
		conn, err = NewTagConn(tag, f.config, f.logf)
		if err != nil {
			return err
		}
		f.connections[tag] = conn
	}

	return conn.Write(entry)
}

func asyncDo(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		fn()
		wg.Done()
	}()
}

func (f *fluentd) Close() {
	for _, conn := range f.connections{
		asyncDo(&f.waitGroup, conn.Close)
	}
	f.waitGroup.Wait()
}

func (f *fluentd) CloseAtOnce() {
	for _, conn := range f.connections{
		conn.CloseAtOnce()
	}
}