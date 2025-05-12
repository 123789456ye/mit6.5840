package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
	//"golang.org/x/mod/sumdb/storage"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.RWMutex
	kvs map[string]string
	versions map[string]rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
        kvs: make(map[string]string),
        versions: make(map[string]rpc.Tversion),
    }
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	v, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = v
		reply.Version = kv.versions[args.Key]
		reply.Err = rpc.OK
	} else {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	curver, ok := kv.versions[args.Key]
	if ok {
		if curver == args.Version {
			kv.kvs[args.Key] = args.Value
			kv.versions[args.Key] = args.Version + 1
			reply.Err = rpc.OK
		} else {
			//golog.Printf("Put Version %d Current Version %d", args.Version, curver)
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.kvs[args.Key] = args.Value
			kv.versions[args.Key] = 1
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
