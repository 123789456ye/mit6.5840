package kvraft

import (
	"sync"
	"sync/atomic"
	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"

)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu      sync.Mutex
	kvStore map[string]string
	kvVersion map[string]rpc.Tversion
	clientTable map[int64]OperationContext
}

type OperationContext struct {
    RequestId int
    Response  any
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
    defer kv.mu.Unlock()

	switch cmd := req.(type) {
	case rpc.GetArgs:

		if context, exists := kv.clientTable[cmd.ClientId]; exists && context.RequestId == cmd.RequestId {
            return context.Response
        }

		reply := rpc.GetReply{}
		key := cmd.Key

		if value, exists := kv.kvStore[key]; exists {
			reply.Value = value
			reply.Version = kv.kvVersion[key]
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}

		kv.clientTable[cmd.ClientId] = OperationContext{
            RequestId: cmd.RequestId,
            Response:  reply,
        }

		return reply

	case rpc.PutArgs:

		if context, exists := kv.clientTable[cmd.ClientId]; exists && context.RequestId == cmd.RequestId {
            return context.Response
        }

		reply := rpc.PutReply{}
        key, value, version := cmd.Key, cmd.Value, cmd.Version

		curVersion, exists := kv.kvVersion[key]

		if !exists {
			if version != 0 {
				reply.Err = rpc.ErrVersion
			} else {
				kv.kvStore[key] = value
				kv.kvVersion[key] = 1
				reply.Err = rpc.OK
			}
		} else {
			if version != curVersion {
				reply.Err = rpc.ErrVersion
			} else {
				kv.kvStore[key] = value
				kv.kvVersion[key] = curVersion + 1
				reply.Err = rpc.OK
			}
		}

		kv.clientTable[cmd.ClientId] = OperationContext{
            RequestId: cmd.RequestId,
            Response:  reply,
        }

		return reply
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
    defer kv.mu.Unlock()
    
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    
    e.Encode(kv.kvStore)
    e.Encode(kv.kvVersion)
    e.Encode(kv.clientTable)
    
    return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) < 1 {
        return
    }
    
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    
    var kvStore map[string]string
    var kvVersion map[string]rpc.Tversion
    var clientTable map[int64]OperationContext
    
    if d.Decode(&kvStore) != nil ||
       d.Decode(&kvVersion) != nil ||
       d.Decode(&clientTable) != nil {
        // Error handling
		return
    } else {
        kv.mu.Lock()
        kv.kvStore = kvStore
        kv.kvVersion = kvVersion
        kv.clientTable = clientTable
        kv.mu.Unlock()
    }
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, result := kv.rsm.Submit(*args)
    
    if err != rpc.OK {
        reply.Err = err
        return
    }
    
    r := result.(rpc.GetReply)
    reply.Value = r.Value
    reply.Version = r.Version
    reply.Err = r.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, result := kv.rsm.Submit(*args)
    
    if err != rpc.OK {
        reply.Err = err
        return
    }
    
    r := result.(rpc.PutReply)
    reply.Err = r.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.GetReply{})
    labgob.Register(rpc.PutReply{})
	labgob.Register(OperationContext{})

	kv := &KVServer{
        me:        me,
        kvStore:   make(map[string]string),
        kvVersion: make(map[string]rpc.Tversion),
		clientTable: make(map[int64]OperationContext),
    }


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
