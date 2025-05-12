package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	key string
	val string
	ver rpc.Tversion
	hold bool
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l, val: kvtest.RandValue(8), ver: 0, hold: false}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if (err == rpc.OK && val == "") || err == rpc.ErrNoKey {
			if ok := lk.ck.Put(lk.key, lk.val, ver); ok == rpc.OK {
				lk.hold = true
				lk.ver = ver + 1
				return
			} else if ok == rpc.ErrMaybe {
				val, _ , err := lk.ck.Get(lk.key);
				if err == rpc.OK && val == lk.val {
					lk.hold = true
					lk.ver = ver + 1
					return
				}
			}
		}
		time.Sleep(10*time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		if ok := lk.ck.Put(lk.key, "", lk.ver); ok == rpc.OK {
			lk.hold = false
			return
		} else if ok == rpc.ErrMaybe {
			val, _ , err := lk.ck.Get(lk.key);
			if err == rpc.OK && val == "" {
				lk.hold = false
				return
			}
		}
		time.Sleep(10*time.Millisecond)
	}
}
