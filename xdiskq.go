// nsq disk queue src code
// author: baoqiang
// time: 2019-04-24 17:49
package xdiskq

import (
	"bytes"
	"fmt"
	"go-common/app/service/ops/log-agent/pkg/bufio"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// TODO 7/20
// diskq一共20个函数，26个field，外面两个函数
type diskQueue struct {
	// 外部传入的元信息
	name            string        // 队列的名称
	dataPath        string        // 磁盘文件存储的路径
	maxBytesPerFile int64         // 每个磁盘文件最大的大小
	syncEvery       int64         // 多久同步一次，每隔多少次同步一次
	syncTimeout     time.Duration // 每次同步最长耗时的时间

	// 读写第几个文件和文件指针的位置，动态变化，存储在文件的元信息里面
	readFileNum  int64
	writeFileNum int64
	readPos      int64
	writePos     int64
	depth        int64

	// 下一次文件的读取位置
	nextReadFileNum int64
	nextReadPos     int64

	//状态变量
	needSync bool
	exitFlag int32

	// chan's
	// TODO 每个chan的作用
	readChan          chan []byte
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int

	// 读写文件
	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// 锁
	sync.Mutex
}

// 新建一个队列实例
func newDiskQueue(name string, dataPath string, maxBytesPerFile int64,
	syncEvery int64, syncTimeout time.Duration) *diskQueue {

	// 实例化一个diskQueue实例
	d := diskQueue{
		name:            name,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		syncEvery:       syncEvery,
		syncTimeout:     syncTimeout,

		//chan's初始化
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
	}

	log.Printf("we create a  DiskQueue(%s), maxBytesPerFile: %d, syncEvery: %d, syncTimeout:%d, dataPath:%s\n", d.name, d.maxBytesPerFile, d.syncEvery, d.syncTimeout, d.dataPath)

	// 读取元数据的信息，不是文件不存在的err
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		log.Printf("[ERROR] DiskQueue(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	// go ioLoop
	go d.ioLoop()

	return &d
}

// publish函数 start
// 写数据
func (d *diskQueue) Put(data []byte) error {
	return nil
}

// 读数据
func (d *diskQueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *diskQueue) Close() error {
	return nil
}

func (d *diskQueue) delete() error {
	return nil
}

func (d *diskQueue) Empty() error {
	return nil
}

func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// publish函数 end

//ioLoop start
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	// 计时器
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// 运行同步的间隔次数
		if count == d.syncEvery {
			count = 0
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				log.Printf("[ERROR] DiskQueue(%s) failed to sync - %s", d.name, err)
			}
		}

		// 读落后于写，或者说有可读的内容的时候才读
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			// 这时候可以读下一部分的内容
			// TODO 这里的if判断是啥意思
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					log.Printf("[ERROR] reading from DiskQueue(%s) at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			// 往读流里面放数据
			r = d.readChan
		} else {
			// 否则没有内容可读
			r = nil
		}

		// select chan
		select {
		//TODO 每个select分支做的事情
		case r <- dataRead:
			d.moveForward()
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count > 0 {
				count = 0
				d.needSync = true
			}
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("[WARN] DiskQueue(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}

//ioLoop end

// io start
func (d *diskQueue) readOne() ([]byte, error) {
	return nil, nil
}

func (d *diskQueue) writeOne(data []byte) error {
	return nil
}

func (d *diskQueue) sync() error {
	return nil
}

// 从文件读取meta信息
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	// 打开meta文件
	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	//读取meta文件里面的元信息
	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}

	//存储元信息和赋值下次读取的位置
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// 存储meta信息到文件
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	// 新建meta文件
	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	//存储meta信息
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		f.Close()
		return err
	}

	f.Sync()
	f.Close()

	return atomicRename(tmpFileName, fileName)
}

func (d *diskQueue) deleteAllFiles() error {
	return nil
}

// io end

// 流程函数 start
func (d *diskQueue) moveForward() {

}

func skipToNextRWFile() error {
	return nil
}

func (d *diskQueue) handleReadError() {

}

func (d *diskQueue) checkTailCorruption(depth int64) {

}

func (d *diskQueue) exit(deleted bool) error {
	return nil
}

// 流程函数 end

//工具函数 start

// meta文件名
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.DiskQueue.meta.dat"), d.name)
}

// 数据文件名
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.DiskQueue.%06d.dat"), d.name, fileNum)
}

//工具函数 end

// 原子交换文件名
func atomicRename(sourceFile, targetFile string) error {
	return os.Rename(sourceFile, targetFile)
}
