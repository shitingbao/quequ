package queue

/*
 @File : queue.go
 @Description: an bounded lock free queue use slice as circle queue
               clone from https://github.com/yireyun/go-queue & https://github.com/bighunter513/goqueue and changed
 @Time : 2022/5/29
 @Update:
*/

import (
	"fmt"
	"runtime"

	"go.uber.org/atomic"
)

const (
	MinCap  = 8   // 最小队列长度，防止队列过小，竞争太激烈； 理论上越大冲突越小
	MaxWait = 100 // 当出现饥饿竞态时，最多让出cpu的次数
)

type IQueue interface {
	Info() string
	Capacity() uint32
	Count() uint32
	Put(val interface{}) (ok bool, count uint32)
	RetryPut(val interface{}, retry uint32) (ok bool, count uint32)

	Get() (val interface{}, ok bool, count uint32)
	RetryGet(retry uint32) (val interface{}, ok bool, count uint32)

	Gets(values []interface{}) (gets, count uint32)
	Puts(values []interface{}) (puts, count uint32)
}

// slot of queue, each slot has a putID and getID
// when new value put, putID will increase by cap, and that mean's it's has value
// only getID + cap == putID, can get value from this slot, then getID increase by cap
// when getID == putID, this slot is empty
//队列的槽，每个槽有一个putID和getID
//当输入新值时，putID将增加cap，这意味着它有值
//只有getID+cap==putID，才能从此插槽中获取值，然后getID增加cap，cap是队列容量，加上cap是为了对应下次读写的位置再到该位置
//当getID==putID时，此插槽为空
type slot struct {
	putID *atomic.Uint32
	getID *atomic.Uint32
	value interface{}
}

// LFQueue An bounded lock free Queue
type LFQueue struct {
	capacity uint32         // const after init, always 2's power，初始化后的常量，始终为2的幂
	capMod   uint32         // cap - 1, const after init，因为 capacity为2的幂次，减1后所有位为0，用于位运算，代替取余操作
	putPos   *atomic.Uint32 // 写入位置
	getPos   *atomic.Uint32 // 读取位置
	carrier  []slot         // 环形数据队列基础数据模型
}

// NewQueue alloc a fixed size of cap Queue
// and do some essential init
func NewQueue(cap uint32) IQueue {
	if cap < 1 {
		cap = MinCap
	}
	q := new(LFQueue)
	q.capacity = minRoundNumBy2(cap)
	q.capMod = q.capacity - 1
	q.putPos = atomic.NewUint32(0)
	q.getPos = atomic.NewUint32(0)
	q.carrier = make([]slot, q.capacity)

	// putID/getID 提前分配好，每用一个，delta增加一轮
	tmp := &q.carrier[0]
	tmp.putID = atomic.NewUint32(q.capacity)
	tmp.getID = atomic.NewUint32(q.capacity)

	var i uint32 = 1
	for ; i < q.capacity; i++ {
		tmp = &q.carrier[i]
		tmp.getID = atomic.NewUint32(i)
		tmp.putID = atomic.NewUint32(i)
	}

	return q
}

// Put May failed if lock slot failed or full
// caller should retry if failed
// should not put nil for normal logic
func (q *LFQueue) Put(val interface{}) (ok bool, count uint32) {
	getPos := q.getPos.Load()
	putPos := q.putPos.Load()

	cnt := q.posCount(getPos, putPos)
	// 如果满了，就直接失败
	if cnt >= q.capMod-1 {
		runtime.Gosched() // 当有其他待执行的逻辑时，比如有很多其他 Put，这里能马上给其他put使用，有空了再来return
		return false, cnt
	}

	// 先占一个坑，如果占坑失败，就直接返回
	posNext := putPos + 1
	if !q.putPos.CAS(putPos, posNext) {
		runtime.Gosched()
		return false, cnt
	}

	var cache *slot = &q.carrier[posNext&q.capMod] // 位操作与上 capMod 对应取余操作，capMod 比 队列长度少1，所以最高位位0，去掉最高位的操作就是取余
	var waitCounter = 0
	for {
		getID := cache.getID.Load()
		putID := cache.putID.Load()
		if posNext == putID && getID == putID { //  posNext == putID 是因为putID 一开始初始化的时候就加入了顺序的位置，和读写的位置相对应，下面加上一个容量的长度，含义是，读写的位置再次读到这个位置，已经是下一轮了
			cache.value = val
			cache.putID.Add(q.capacity)
			return true, cnt + 1
		} else {
			// 存线程的竞争过多，而队列cap过小，前面的如果写数据动作比较慢，而后来的进程已经lock到这个位置的下一轮了
			// 此时，这个位置等于已经被他预约了，但是数据还没取走，需要等待下次get了数据之后，才能重新put
			// 所以就先让出cpu，等待下次调度
			// 为啥不直接返回？ 因为位置已经占了，其他线程不会占用这个地方了
			waitCounter++
			fmt.Printf("put too quick: getID %v, putID %v and putPosNext: %v, wait: %v\n", getID, putID, posNext, waitCounter)
			if waitCounter > MaxWait {
				// 实在put不进去，一直没有消费, 那就扔一条吧, 这里主要是防止调用进程死等, 理论上极小概率到这里
				val, ok, cnt := q.Get()
				if ok {
					fmt.Printf("throw val: %v away, cnt: %v\n", val, cnt)
					continue
				}
			}
			runtime.Gosched()
		}

	}
}

// Get May failed if lock slot failed or empty
// caller should retry if failed, val nil also means false
func (q *LFQueue) Get() (val interface{}, ok bool, count uint32) {
	getPos := q.getPos.Load()
	putPos := q.putPos.Load()

	cnt := q.posCount(getPos, putPos)
	if cnt < 1 {
		runtime.Gosched()
		return nil, false, cnt
	}

	getPosNext := getPos + 1
	if !q.getPos.CAS(getPos, getPosNext) {
		runtime.Gosched()
		return nil, false, cnt
	}

	cache := &q.carrier[getPosNext&q.capMod]

	var waitCounter = 0
	for {
		getID := cache.getID.Load()
		putID := cache.putID.Load()
		if getPosNext == getID && (getID+q.capacity == putID) {
			val = cache.value
			cache.value = nil
			cache.getID.Add(q.capacity)
			ret := true
			if val == nil {
				ret = false
			}
			return val, ret, cnt - 1
		} else {
			// 可能是取的竞争过多，而队列cap过小，前面的如果取数据动作比较慢，而后来的进程已经取到这个位置的下一轮了
			// 此时，这个位置等于已经被他预约了，但是却没数据，需要等待下次put了数据之后，才能重新取到
			// 所以就先让出cpu，等待下次调度
			waitCounter++
			fmt.Printf("get too quick: getID %v, putID %v and getPosNext: %v, wait: %v\n", getID, putID, getPosNext, waitCounter)
			if waitCounter > MaxWait {
				// 实在get不到，一直没有put, 那就put一个假数据吧, 这里主要是防止调用进程死等, 理论上极小概率到这里
				ok, _ := q.Put(nil)
				if ok {
					fmt.Printf("put nil to escape\n")
					continue
				}
			}
			runtime.Gosched()
		}
	}
}

// minRoundNumBy2 round 到 >=N的 最近的2的倍数，
// example f(3) = 4
func minRoundNumBy2(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}
