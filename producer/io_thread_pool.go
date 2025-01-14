package producer

import (
	"container/list"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
)

type IoThreadPool struct {
	threadPoolShutDownFlag *atomic.Bool
	queue                  *list.List
	lock                   sync.RWMutex
	ioworker               *IoWorker
	logger                 log.Logger
}

func initIoThreadPool(ioworker *IoWorker, logger log.Logger) *IoThreadPool {
	return &IoThreadPool{
		threadPoolShutDownFlag: atomic.NewBool(false),
		queue:                  list.New(),
		ioworker:               ioworker,
		logger:                 logger,
	}
}

func (threadPool *IoThreadPool) addTask(batch *ProducerBatch) {
	// 先defer再获取锁(思路清奇) ???
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	// 增加数据没有做任何的限制
	// 如果底层work慢导致无协程可用就不能及时处理内存会急速增加甚至是OOM ???
	threadPool.queue.PushBack(batch)
}

func (threadPool *IoThreadPool) popTask() *ProducerBatch {
	// 先defer再获取锁(思路清奇) ???
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	if threadPool.queue.Len() <= 0 {
		return nil
	}
	ele := threadPool.queue.Front()
	threadPool.queue.Remove(ele)
	return ele.Value.(*ProducerBatch)
}

func (threadPool *IoThreadPool) hasTask() bool {
	// 先defer再获取锁(思路清奇) ???
	defer threadPool.lock.RUnlock()
	threadPool.lock.RLock()
	return threadPool.queue.Len() > 0
}

func (threadPool *IoThreadPool) start(ioWorkerWaitGroup *sync.WaitGroup, ioThreadPoolwait *sync.WaitGroup) {
	defer ioThreadPoolwait.Done()
	for {
		if task := threadPool.popTask(); task != nil {
			threadPool.ioworker.startSendTask(ioWorkerWaitGroup)
			// 具体的work应该在io_work部分包装更合理 ???
			go func(producerBatch *ProducerBatch) {
				defer threadPool.ioworker.closeSendTask(ioWorkerWaitGroup)
				threadPool.ioworker.sendToServer(producerBatch)
			}(task)
		} else {
			if !threadPool.threadPoolShutDownFlag.Load() {
				time.Sleep(100 * time.Millisecond)
			} else {
				level.Info(threadPool.logger).Log("msg", "All cache tasks in the thread pool have been successfully sent")
				break
			}
		}
	}

}
