package queue

/*
 @File : queue.go
 @Description: an bounded lock free queue use slice as circle queue
               clone from https://github.com/yireyun/go-queue & https://github.com/bighunter513/goqueue and changed
 @Time : 2022/5/29
 @Update:
*/

import (
	"runtime"

	"go.uber.org/atomic"
)

var MinCap uint32 = 8 // 最小队列长度，防止队列过小，竞争太激烈； 理论上越大冲突越小
// MaxWait = 100 // 当出现饥饿竞态时，最多让出cpu的次数

type Queue interface {
	// Info() string
	// Capacity() uint32
	// Count() uint32
	Put(val interface{}) (ok bool, count uint32)
	// RetryPut(val interface{}, retry uint32) (ok bool, count uint32)

	Get() (val interface{}, ok bool, count uint32)
	// RetryGet(retry uint32) (val interface{}, ok bool, count uint32)

	// Gets(values []interface{}) (gets, count uint32)
	// Puts(values []interface{}) (puts, count uint32)
}

// 队列的槽，每个槽有一个 writeID 和 readID
// 当输入新值时，putID将增加cap，这意味着它有值
// 只有 readID + cap == writeID ，才能从此插槽中获取值，然后 readID 增加 cap，cap 是队列容量，加上 cap 是为了对应下次读写的位置再到该位置
// 当 readID == writeID 时，此插槽为空
type slot struct {
	writeID *atomic.Uint32 // write + n 倍的容量
	readID  *atomic.Uint32 // write + n 倍的容量
	value   interface{}
}

// DefaultQueue An bounded lock free Queue
type DefaultQueue struct {
	cap     uint32         // const after init, always 2's power，初始化后的常量，始终为2的幂
	capMod  uint32         // cap - 1, const after init，因为 capacity为2的幂次，减1后所有位为0，用于位运算，代替取余操作
	write   *atomic.Uint32 // 写入位置
	read    *atomic.Uint32 // 读取位置
	carrier []slot         // 环形数据队列基础数据模型
}

// NewQueue alloc a fixed size of cap Queue
// and do some essential init
func NewQueue(cap uint32) Queue {

	q := new(DefaultQueue)
	q.cap = q.minRoundNumBy2(cap)
	q.capMod = q.cap - 1
	q.write = atomic.NewUint32(0)
	q.read = atomic.NewUint32(0)
	q.carrier = make([]slot, q.cap)

	// writeID/readID 提前分配好，每用一个，delta增加一轮
	tmp := &q.carrier[0]
	tmp.writeID = atomic.NewUint32(q.cap)
	tmp.readID = atomic.NewUint32(q.cap)

	var i uint32 = 1
	for ; i < q.cap; i++ {
		tmp = &q.carrier[i]
		tmp.readID = atomic.NewUint32(i)
		tmp.writeID = atomic.NewUint32(i)
	}

	return q
}

// Put May failed if lock slot failed or full
// caller should retry if failed
// should not put nil for normal logic
func (q *DefaultQueue) Put(val interface{}) (ok bool, count uint32) {
	read := q.read.Load()
	write := q.write.Load()

	cnt := q.posCount(read, write)
	// 如果满了，就直接失败
	if cnt >= q.capMod-1 {
		runtime.Gosched() // 当有其他待执行的逻辑时，比如有很多其他 Put，这里能马上给其他put使用，有空了再来return
		return false, cnt
	}

	// 先占一个坑，如果占坑失败，就直接返回
	posNext := write + 1
	if !q.write.CAS(write, posNext) {
		runtime.Gosched()
		return false, cnt
	}

	var cache *slot = &q.carrier[posNext&q.capMod] // 位操作与上 capMod 对应取余操作，capMod 比 队列长度少1，所以最高位位0，去掉最高位的操作就是取余
	// var waitCounter = 0
	for {
		readID := cache.readID.Load()
		writeID := cache.writeID.Load()

		// posNext == writeID 是因为putID 一开始初始化的时候就加入了顺序的位置，和读写的位置相对应，下面加上一个容量的长度，含义是，读写的位置再次读到这个位置，已经是下一轮了
		// readID == writeID 表示还是空的，如果有写入 writeID 就会 add 一个长度就会比 readID 大，由此来标记获取到锁后该槽是否为空
		// 同时这里为什么放在 for 里面也是这个原因，可能情况是读的操作到了这个槽的位置，但是他还没来得及写进去（已经获取到锁的状态），那就要for 多试几次，读和写同理
		if posNext == writeID && readID == writeID {
			cache.value = val
			cache.writeID.Add(q.cap) // 为什么不需要锁，当读与写都在一个位置的时候，因为这里加上一个 cap，在 get 的时候判断是否有增加过 cap 来判断是否已经赋值完毕，光获取到当前的 write 标识还不够
			return true, cnt + 1
		} else {
			// @review 是否要加失败跳出待定

			// 存线程的竞争过多，而队列cap过小，前面的如果写数据动作比较慢，而后来的进程已经lock到这个位置的下一轮了
			// 此时，这个位置等于已经被他预约了，但是数据还没取走，需要等待下次get了数据之后，才能重新put
			// 所以就先让出cpu，等待下次调度
			// 为啥不直接返回？ 因为位置已经占了，其他线程不会占用这个地方了
			// =======================================================
			// waitCounter++
			// fmt.Printf("put too quick: readID %v, writeID %v and putPosNext: %v, wait: %v\n", readID, writeID, posNext, waitCounter)
			// if waitCounter > MaxWait {
			// 	// 实在put不进去，一直没有消费, 丢弃一条数据, 这里主要是防止调用进程死等, 理论上极小概率到这里
			// 	val, ok, cnt := q.Get()
			// 	if ok {
			// 		fmt.Printf("throw val: %v away, cnt: %v\n", val, cnt)
			// 		continue
			// 	}
			// }
			// =======================================================
			// 本来为了让其他操作不过度等待加的数据丢弃，发现这部分在大量 put （出现内扣一圈，这个位置又被put），不能保证原子性（因为获取锁和写入分离）
			// 这个 else 里的 q.Get() 不能保证取到放入的数据，可能数据还没放进去
			// 也就是说，如果在大量写入的情况下，相同位置被下一个循环覆盖写入
			runtime.Gosched()
		}

	}
}

// Get May failed if lock slot failed or empty
// caller should retry if failed, val nil also means false
func (q *DefaultQueue) Get() (val interface{}, ok bool, count uint32) {
	read := q.read.Load()
	write := q.write.Load()

	cnt := q.posCount(read, write)
	if cnt < 1 {
		runtime.Gosched()
		return nil, false, cnt
	}

	getPosNext := read + 1
	if !q.read.CAS(read, getPosNext) {
		runtime.Gosched()
		return nil, false, cnt
	}

	cache := &q.carrier[getPosNext&q.capMod]

	// var waitCounter = 0
	for {
		readID := cache.readID.Load()
		writeID := cache.writeID.Load()
		if getPosNext == readID && (readID+q.cap == writeID) {
			val = cache.value
			cache.value = nil
			cache.readID.Add(q.cap)
			ret := true
			if val == nil {
				ret = false
			}
			return val, ret, cnt - 1
		} else {
			runtime.Gosched()
		}
	}
}

// 队列中元素的个数，注意读写指标前后位置
func (q *DefaultQueue) posCount(read, write uint32) uint32 {
	if write > read {
		return write - read
	}
	return q.cap - read + write
}

// minRoundNumBy2 round 到 >=N的 最近的2的倍数，
// example f(3) = 4
func (q *DefaultQueue) minRoundNumBy2(v uint32) uint32 {
	if v < MinCap {
		v = MinCap
	}

	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16

	if ^v == 0 {
		v-- // --为了防止刚刚好最大值时溢出，上列操作后所有位上都是1，最后 ++ 如果一开始就是占了最大的长度，就会溢出
	} else {
		v++
	}
	return v
}
