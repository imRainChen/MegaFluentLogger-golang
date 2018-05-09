package fluentd

import (
	"runtime"
	"testing"
	"reflect"
	"path/filepath"
	"fmt"
	"github.com/nsqio/go-diskqueue"
	"net"
	"github.com/ugorji/go/codec"
	"time"
	"os"
	"io/ioutil"
	"io"
)

func Equal(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   %#v (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func NotEqual(t *testing.T, expected, actual interface{}) {
	if reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func Nil(t *testing.T, object interface{}) {
	if !isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   <nil> (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, object)
		t.FailNow()
	}
}

func NotNil(t *testing.T, object interface{}) {
	if isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tExpected value not to be <nil>\033[39m\n\n",
			filepath.Base(file), line)
		t.FailNow()
	}
}

func isNil(object interface{}) bool {
	if object == nil {
		return true
	}

	value := reflect.ValueOf(object)
	kind := value.Kind()
	if kind >= reflect.Chan && kind <= reflect.Slice && value.IsNil() {
		return true
	}

	return false
}


type Foo struct{
	Bar int	`codec:"bar"`
}

type tbLog interface {
	Log(...interface{})
}

func NewTestLogger(tbl tbLog) diskqueue.AppLogFunc {
	return func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		tbl.Log(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
}

func init()  {
	numProcs := runtime.NumCPU()
	if numProcs < 2 {
		numProcs = 2
	}
	runtime.GOMAXPROCS(numProcs)

	listener, err := net.Listen("tcp", "0.0.0.0:6666")
	if err != nil {
		println("error listening:", err.Error())
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				println("Error accept:", err.Error())
				return
			}
			go EchoFunc(conn)
		}
	}()
}

func EchoFunc(conn net.Conn) {
	hd := new(codec.MsgpackHandle)
	hd.RawToString = true
	var f Forward
	decoder := codec.NewDecoder(conn, hd)
	encoder := codec.NewEncoder(conn, hd)
	for {
		err := decoder.Decode(&f)
		if  err != io.EOF && err != nil {
			println("Error reading:", err.Error())
			return
		}

		resp := make(map[string]string)
		resp["ack"] = f.Option.Chunk
		err = encoder.Encode(resp)
		if err != nil {
			println("Error write:", err.Error())
			return
		}
	}
}

func TestFluentd_Push(t *testing.T) {
	l := NewTestLogger(t)
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("test-log-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := New(Config{
		FluentPort: 6666,
		BufferDataPath: tmpDir,
	}, l)

	if err != nil {
		t.Error(err)
		return
	}

	NotNil(t, f)

	tag := "log.test"
	f.Push(tag, Foo{ 1 })

	f.Close()
	Equal(t, "", f.connections[tag].curAck)
}
