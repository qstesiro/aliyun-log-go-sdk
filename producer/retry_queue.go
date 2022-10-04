package producer

import (
	"container/heap"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// RetryQueue cache ProducerBatch and retry latter
// 按时间排序的小顶堆实现,值得借鉴 !!!
type RetryQueue struct {
	batch []*ProducerBatch
	mutex sync.Mutex
}

func initRetryQueue() *RetryQueue {
	retryQueue := RetryQueue{}
	heap.Init(&retryQueue)
	return &retryQueue
}

func (retryQueue *RetryQueue) sendToRetryQueue(producerBatch *ProducerBatch, logger log.Logger) {
	level.Debug(logger).Log("msg", "Send to retry queue")
	retryQueue.mutex.Lock()
	defer retryQueue.mutex.Unlock()
	if producerBatch != nil {
		heap.Push(retryQueue, producerBatch)
	}
}

func (retryQueue *RetryQueue) getRetryBatch(moverShutDownFlag bool) (producerBatchList []*ProducerBatch) {
	retryQueue.mutex.Lock()
	defer retryQueue.mutex.Unlock()
	if !moverShutDownFlag { // 正常清理
		for retryQueue.Len() > 0 {
			// pop数据,已经超过等待时长的进行处理,未到达等待时长的再push回堆中并退出
			// 因为是按下次重试的时间点进行小顶堆排序,所以找到第一个大于当前的时间的后续也都是未到过重试时间的
			producerBatch := heap.Pop(retryQueue)
			if producerBatch.(*ProducerBatch).nextRetryMs < GetTimeMs(time.Now().UnixNano()) {
				producerBatchList = append(producerBatchList, producerBatch.(*ProducerBatch))
			} else {
				heap.Push(retryQueue, producerBatch.(*ProducerBatch))
				break
			}
		}
	} else { // 关闭清理
		for retryQueue.Len() > 0 {
			producerBatch := heap.Pop(retryQueue)
			producerBatchList = append(producerBatchList, producerBatch.(*ProducerBatch))
		}
	}
	return producerBatchList
}

func (retryQueue *RetryQueue) Len() int {
	return len(retryQueue.batch)
}

func (retryQueue *RetryQueue) Less(i, j int) bool {
	return retryQueue.batch[i].nextRetryMs < retryQueue.batch[j].nextRetryMs
}
func (retryQueue *RetryQueue) Swap(i, j int) {
	retryQueue.batch[i], retryQueue.batch[j] = retryQueue.batch[j], retryQueue.batch[i]
}
func (retryQueue *RetryQueue) Push(x interface{}) {
	item := x.(*ProducerBatch)
	retryQueue.batch = append(retryQueue.batch, item)
}
func (retryQueue *RetryQueue) Pop() interface{} {
	old := retryQueue.batch
	n := len(old)
	item := old[n-1]
	retryQueue.batch = old[0 : n-1]
	return item
}
