package fluentd

import (
	"net"
	"sync"
	"bytes"
	"fluent-logger-golang/fluentd/diskqueue"
	"strconv"
	"time"
	"math"
	"github.com/ugorji/go/codec"
	"errors"
)

type tagConn struct {
	Config
	tag    string
	curAck string
	waitAck   bool

	writeBuf *bytes.Buffer
	readBuf  *bytes.Buffer
	muBuffer sync.Mutex

	queue    diskqueue.Interface
	useQueue bool

	conn         net.Conn
	connClosed	 bool
	rwMuConn       sync.RWMutex
	connErr      error

	codecHandle     codec.MsgpackHandle
	encoder         *codec.Encoder
	decoder         *codec.Decoder
	writeBufEncoder *codec.Encoder

	exitChan        chan int
	exitFlag        bool
	closeAtOnceFlag bool
	waitGroup       sync.WaitGroup

	logf diskqueue.AppLogFunc
}

func NewTagConn(tag string, config Config, logf diskqueue.AppLogFunc) (*tagConn, error) {
	t := tagConn{
		Config:   config,
		tag:      tag,
		readBuf:  new(bytes.Buffer),
		writeBuf: new(bytes.Buffer),
		queue: diskqueue.New(
			tag,
			config.BufferDataPath,
			4*1024*1024,
			4,
			int32(config.MaxBytesPerBuffer)*2,
			2500,
			2*time.Second,
			logf,
		),
		useQueue:        false,
		waitAck:         false,
		exitFlag:        false,
		closeAtOnceFlag: false,
		exitChan:        make(chan int),
		logf:            logf,
	}

	t.codecHandle.RawToString = true
	t.encoder = codec.NewEncoder(t.readBuf, &t.codecHandle)
	t.writeBufEncoder = codec.NewEncoder(t.writeBuf, &t.codecHandle)

	if t.queue.Depth() > 0 {
		t.useQueue = true
	}

	asyncDo(&t.waitGroup, t.ioLoop)
	return &t, nil
}

func (t *tagConn) connect() (err error) {
	t.rwMuConn.Lock()
	defer t.rwMuConn.Unlock()

	switch t.Config.FluentNetwork {
	case "tcp":
		t.conn, err = net.DialTimeout(t.Config.FluentNetwork, t.Config.FluentHost+":"+strconv.Itoa(t.Config.FluentPort), t.Config.Timeout)
	case "unix":
		t.conn, err = net.DialTimeout(t.Config.FluentNetwork, t.Config.FluentSocketPath, t.Config.Timeout)
	default:
		err = net.UnknownNetworkError(t.Config.FluentNetwork)
	}

	t.connErr = err
	if err == nil {
		t.decoder = codec.NewDecoder(t.conn, &t.codecHandle)
		t.connClosed = false
		t.logf(INFO, "TAG-CONN(%s) connect fluentd", t.tag)
	} else {
		t.logf(ERROR, "TAG-CONN(%s) connect fluentd failed", t.tag)
		t.connClose()
	}

	return
}

func (t *tagConn) reconnect() {
	for i := 0; ; i++ {
		err := t.connect()
		if err == nil {
			return
		}

		if t.exitFlag {
			return
		}

		if i == t.Config.MaxRetry && 0 != t.Config.MaxRetry {
			panic("fluent#reconnect: failed to reconnect!")
		}

		waitTime := t.Config.RetryWait * e(defaultReconnectWaitIncreRate, float64(i-1))
		time.Sleep(time.Duration(waitTime) * time.Millisecond)
	}
}

func (t *tagConn) Write(data interface{}) (err error) {
	t.muBuffer.Lock()
	defer t.muBuffer.Unlock()

	if t.exitFlag {
		return errors.New("exiting")
	}

	err = t.encoder.Encode(data)
	if err != nil {
		return
	}

	if t.readBuf.Len() >= t.Config.MaxBytesPerBuffer {
		err = t.queue.Put(t.readBuf.Bytes())
		if err != nil {
			return
		}

		t.readBuf.Reset()
		t.useQueue = true
	}

	return
}

func (t *tagConn) newForward(entries []byte) *Forward {
	forward := Forward{
		Tag: t.tag,
		Entries: entries,
		Option: Option{ Chunk: strconv.FormatInt(time.Now().UnixNano(), 10) },
	}

	return &forward
}

func (t *tagConn) send() (err error) {
	var data []byte

	if t.waitAck {
		return
	}

	if t.writeBuf.Len() > 0 {
		err = t.sendWriteBuf()
		if err != nil {

		}
		return
	}

	if t.useQueue {
		if t.queue.Depth() > 0 {
			data = <- t.queue.ReadChan()
			goto send
		}

		t.useQueue = false
	}

	t.muBuffer.Lock()
	if t.readBuf.Len() > 0 {
		data = t.readBuf.Bytes()
		t.readBuf.Reset()
	}
	t.muBuffer.Unlock()

	send:
	if data != nil {
		forward := t.newForward(data)
		err = t.writeBufEncoder.Encode(forward)
		if err != nil {
			return
		}

		t.curAck = forward.Option.Chunk
		err = t.sendWriteBuf()
	}

	return
}

func (t *tagConn) sendWriteBuf() (err error) {
	if t.connIsClosed() {
		t.reconnect()
		return t.connErr
	}

	if t.writeBuf.Len() > 0 {
		timeout := t.Config.WriteTimeout
		if time.Duration(0) < timeout {
			t.conn.SetWriteDeadline(time.Now().Add(timeout))
		} else {
			t.conn.SetWriteDeadline(time.Time{})
		}

		_, err := t.conn.Write(t.writeBuf.Bytes())
		if err != nil {
			t.connClose()
			t.waitAck = false
		} else {
			t.logf(INFO, "TAG-CONN(%s) send write buf, ack:%s", t.tag, t.curAck)
			t.waitAck = true
		}
	}

	return
}

func (t *tagConn) receive () (err error) {
	if !t.waitAck {
		return
	}

	if t.connIsClosed() {
		t.reconnect()
		if t.connErr != nil {
			if t.exitFlag {
				t.waitAck = false
			}
			return t.connErr
		}
	}

	timeout := t.Config.WriteTimeout
	if time.Duration(0) < timeout {
		t.conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		t.conn.SetReadDeadline(time.Time{})
	}

	var resp map[string]string
	err = t.decoder.Decode(&resp)
	if err != nil {
		t.connClose()
		t.waitAck = false
		return
	} else {
		if ack, ok := resp["ack"]; ok && ack == t.curAck {
			t.waitAck = false
			t.curAck = ""
			t.writeBuf.Reset()
			t.logf(INFO, "TAG-CONN(%s) ack:%s", t.tag, ack)
		}

		if t.closeAtOnceFlag {
			return
		}

		t.send()
	}

	return
}

func e(x, y float64) int {
	return int(math.Pow(x, y))
}

func  (t *tagConn) connIsClosed() (b bool) {
	if t.conn != nil {
		t.rwMuConn.RLock()
		b = t.connClosed
		t.rwMuConn.RUnlock()
	} else {
		b = true
	}
	return
}

func  (t *tagConn) connClose() error {
	if t.conn == nil || t.connIsClosed() {
		return t.connErr
	}

	t.rwMuConn.Lock()
	t.connClosed = true
	t.connErr = t.conn.Close()
	t.rwMuConn.Unlock()
	return t.connErr
}

func (t *tagConn) ioLoop() {
	t.connect()
	syncTicker := time.NewTicker(time.Second)
	exitFlag := false

	for {
		select {
		case <-t.exitChan:
			syncTicker.Stop()
			exitFlag = true
			if t.closeAtOnceFlag {
				goto exit
			} else {
				t.send()
			}
		case <-syncTicker.C:
			if err := t.send(); err != nil {
				t.logf(ERROR, "TAG-CONN(%s) 发送数据 - %s", t.tag, err)
			}
		default:
			if err := t.receive(); err != nil {
				t.logf(ERROR, "TAG-CONN(%s) 接收数据 - %s", t.tag, err)
			}

			if exitFlag && !t.waitAck {
				goto exit
			}
		}
	}

exit:
	t.logf(INFO, "TAG-CONN(%s) close io loop", t.tag)
}

//func (t *tagConn) asyncDo(fn func()) {
//	t.waitGroup.Add(1)
//	go func() {
//		fn()
//		t.waitGroup.Done()
//	}()
//}

// 立即关闭
// 不等待缓冲区数据发送完毕，会将所有未发送数据写入文件队列
func (t *tagConn) CloseAtOnce() {
	t.closeAtOnceFlag = true
	t.Close()
}

// 关闭连接
// 会等待所有缓冲区数据发送完毕且ack返回后关闭
// 当ack超时则立即关闭，缓冲区数据写入文件队列
func (t *tagConn) Close() {
	t.muBuffer.Lock()
	if t.exitFlag == true {
		return
	}
	t.exitFlag = true
	t.muBuffer.Unlock()

	t.exitChan <- 1
	t.logf(INFO, "TAG-CONN(%s) tag conn wait close", t.tag)

	t.waitGroup.Wait()
	close(t.exitChan)

	if t.writeBuf.Len() > 0 {
		b := new(bytes.Buffer)
		b.Write(t.writeBuf.Bytes())
		f := Forward{}
		d := codec.NewDecoder(b, &t.codecHandle)
		err := d.Decode(&f)
		if err == nil {
			t.queue.Put(f.Entries)
		}
		t.logf(INFO, "TAG-CONN(%s) 输出缓存写入文件", t.tag)
	}

	if t.readBuf.Len() > 0 {
		t.queue.Put(t.readBuf.Bytes())
		t.logf(INFO, "TAG-CONN(%s) 输入缓存写入文件", t.tag)
	}

	t.queue.Close()
	t.connClose()
}