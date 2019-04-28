// 测试
// author: baoqiang
// time: 2019-04-24 18:34
package xdiskq

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestGo(t *testing.T) {
	GoRandom()
}

// uint8  : 0 to 255
// uint16 : 0 to 65535
// uint32 : 0 to 4294967295
// uint64 : 0 to 18446744073709551615
// int8   : -128 to 127
// int16  : -32768 to 32767
// int32  : -2147483648 to 2147483647
// int64  : -9223372036854775808 to 9223372036854775807
//18,446,744,073,709,551,615
//1844,6744,0737,0955,1615
//万兆,兆,亿,万,个
func GoRandom() {
	for range [10]int{} {
		r := rand.Int()
		fmt.Println(r)
	}
}

type tLog interface {
	Log(...interface{})
}

func NewTestLogger(t tLog) AppLogFunc {
	return func(lvl LogLevel, f string, args ...interface{}) {
		t.Log(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
}

func TestDiskQueue(t *testing.T) {
	// 自定义日志
	l := NewTestLogger(t)

	// 队列名称
	dpName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))

	// 临时文件目录
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	// 删除所有临时文件
	defer os.RemoveAll(tmpDir)

	// 新建一个文件队列
	dq := New(dpName, tmpDir, 1024, 4, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	//dp不为空断言
	NotNil(t, dq)
	Equal(t, int64(0), dq.Depth())

	//写一条数据
	msg := []byte("test")
	err = dq.Put(msg)
	//err为空断言
	Nil(t, err)
	Equal(t, int64(1), dq.Depth())

	//读一条数据
	msgOut := <-dq.ReadChan()
	Equal(t, msg, msgOut)
}

// copied assert func from https://github.com/nsqio/go-diskqueue/blob/master/diskqueue_test.go
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
