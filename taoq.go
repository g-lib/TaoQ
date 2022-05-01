package taoq

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

// 队列大小,必须是2的N次方
// 最大=2^64-2
const queueSize uint64 = 4096

const indexMask uint64 = queueSize - 1

type TaoQ[T any] struct {
	padding1           [8]uint64
	lastCommittedIndex uint64
	padding2           [8]uint64
	nextFreeIndex      uint64
	padding3           [8]uint64
	readerIndex        uint64
	padding4           [8]uint64
	contents           [queueSize]T
	padding5           [8]uint64
}

// New 返回给定类型的新队列
func New[T any]() *TaoQ[T] {
	return &TaoQ[T]{lastCommittedIndex: 0, nextFreeIndex: 1, readerIndex: 1}
}

func (self *TaoQ[T]) Write(value T) {
	myIndex := atomic.AddUint64(&self.nextFreeIndex, 1) - 1
	for myIndex > (atomic.LoadUint64(&self.readerIndex) + queueSize - 2) {
		runtime.Gosched()
	}
	self.contents[myIndex&indexMask] = value

	for !atomic.CompareAndSwapUint64(&self.lastCommittedIndex, myIndex-1, myIndex) {
		runtime.Gosched()
	}
}

func (self *TaoQ[T]) Read() T {
	var myIndex = atomic.AddUint64(&self.readerIndex, 1) - 1
	for myIndex > atomic.LoadUint64(&self.lastCommittedIndex) {
		runtime.Gosched()
	}
	return self.contents[myIndex&indexMask]
}

func (self *TaoQ[T]) Dump() {
	fmt.Printf("lastCommitted:%3d,nextFree:%3d,readerIndex:%3dcontents:", self.lastCommittedIndex, self.nextFreeIndex, self.readerIndex)
	for index, value := range self.contents {
		fmt.Printf("%5v:%5v", index, value)
	}
	fmt.Println()
}
