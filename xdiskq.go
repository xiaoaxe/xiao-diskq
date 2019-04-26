// nsq disk queue src code
// author: baoqiang
// time: 2019-04-24 17:49
package xdiskq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// REF
// https://github.com/nsqio/go-diskqueue/blob/master/diskqueue.go
//
// https://swanspouse.github.io/2018/11/27/nsq-disk-queue/
//

// TODO 17/20
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
	sync.RWMutex
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
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("existing")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// 读数据
func (d *diskQueue) ReadChan() chan []byte {
	return d.readChan
}

// 关闭之前同步文件
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}

	return d.sync()
}

// 不保存直接关闭
func (d *diskQueue) Delete() error {
	return d.exit(true)
}

// 删除所有的文件
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("existing")
	}

	log.Printf("[WARN] DiskQueue(%s): emptying", d.name)

	// 给ioLoop发送清空的信号
	d.emptyChan <- 1

	return <-d.emptyResponseChan
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
	var err error
	var msgSize int32

	// 打开文件
	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		log.Printf("DiskQueue(%s): readOne() opened %s", d.name, curFileName)

		// 找到上次读取的文件的结束位置
		if d.readPos > 0 {
			// seek游标开始的位置: 0-开头 1-当前位置 2-末尾
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		//包装文件读取流
		d.reader = bufio.NewReader(d.readFile)
	}

	// 大端读取(低位地址内存的高位地址存放)
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	// 读取msgSize的数据
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	//头部的msgSize为int32占用了4个字节
	totalBytes := int64(4 + msgSize)

	//设置下一个读取的位置
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	//如果超过了大小，读取下一个文件
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

func (d *diskQueue) writeOne(data []byte) error {
	var err error

	// 打开文件
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		// TODO 权限位为什么是766
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0766)
		if err != nil {
			return err
		}

		log.Printf("DiskQueue(%s): writeOne() opened %s", d.name, curFileName)

		// 找下一个写的位置
		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	// 写入长度
	dataLen := len(data)

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, int32(dataLen))

	// 先写缓存，再写入数据
	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	//写入数据的下一个位置
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	// depth + 1
	atomic.AddInt64(&d.depth, 1)

	//如果写满了，写下一个文件
	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// 写meta文件
		err = d.sync()
		if err != nil {
			log.Printf("[ERROR] DiskQueue(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}

	}

	return err
}

// 写meta文件，并且写文件入磁盘
func (d *diskQueue) sync() error {
	// 数据落盘
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	// 存储元数据
	err := d.persistMetaData()
	if err != nil {
		return err
	}

	// 同步完了就不需要同步了
	d.needSync = false

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
	// 删除数据文件
	err := d.skipToNextRWFile()

	// 删除元文件
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		log.Printf("[ERROR] DiskQueue(%s) failed to remove metadata file - %s", d.name, err)
		return innerErr
	}

	return err
}

// io end

// 流程函数 start
func (d *diskQueue) moveForward() {

}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	// 关闭读流
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	// 关闭写流
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	// 删除目前还没有读取的所有数据
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			log.Printf("[ERROR] DiskQueue(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	// 数据归位
	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

func (d *diskQueue) handleReadError() {

}

func (d *diskQueue) checkTailCorruption(depth int64) {

}

// exist 表示退出之前是否同步chan里面目前存在的数据
func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	// 标记已经退出
	d.exitFlag = 1

	if deleted {
		log.Printf("[WARN] DiskQueue(%s): deleting", d.name)
	} else {
		log.Printf("[WARN] DiskQueue(%s): closing", d.name)
	}

	// 关闭通道保证select分支可以走到
	close(d.exitChan)

	// 确保ioLoop函数可以正常退出
	<-d.exitSyncChan

	log.Printf("exit <<<<<< %d", d.writePos)

	// 关闭文件
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

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
