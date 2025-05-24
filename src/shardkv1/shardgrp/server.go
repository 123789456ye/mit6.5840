package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)


type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu      sync.Mutex
	kvStore map[string]string
	kvVersion map[string]rpc.Tversion
	clientTable map[int64]OperationContext

	ownedShards map[shardcfg.Tshid]bool 
    maxShardNums map[shardcfg.Tshid]shardcfg.Tnum
	frozenShards map[shardcfg.Tshid]bool
}

type OperationContext struct {
    RequestId int
    Response  any
}

type KeyValue struct {
    Value   string
    Version rpc.Tversion
}


func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
    defer kv.mu.Unlock()

	switch cmd := req.(type) {
	case rpc.GetArgs:

		shard := shardcfg.Key2Shard(cmd.Key)
        if !kv.ownedShards[shard] || kv.frozenShards[shard] {
            reply := rpc.GetReply{Err: rpc.ErrWrongGroup}
            return reply
        }

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

		shard := shardcfg.Key2Shard(cmd.Key)
        if !kv.ownedShards[shard] || kv.frozenShards[shard] {
            reply := rpc.PutReply{Err: rpc.ErrWrongGroup}
            return reply
        }

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

	case shardrpc.FreezeShardArgs:
        shard := cmd.Shard
        reply := shardrpc.FreezeShardReply{}
        
        // Check for duplicate request
        clientId := cmd.ClientId
        requestId := cmd.RequestId
        if lastOp, ok := kv.clientTable[clientId]; ok && lastOp.RequestId == requestId {
            return lastOp.Response
        }
        
        // Update max config num
        if kv.maxShardNums[shard] < cmd.Num {
            kv.maxShardNums[shard] = cmd.Num
        } else if kv.maxShardNums[shard] > cmd.Num {
            reply.Err = rpc.ErrMaybe
            kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
            return reply
        }
        
        // Check if this server owns the shard
        if !kv.ownedShards[shard] {
            reply.Err = rpc.ErrWrongGroup
            kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
            return reply
        }
        
        // Mark shard as frozen
        kv.frozenShards[shard] = true
        
        // Collect data for just this shard - efficient state transfer
        shardData := make(map[string]KeyValue)
        for key, value := range kv.kvStore {
            if shardcfg.Key2Shard(key) == shard {
                shardData[key] = KeyValue{
                    Value:   value,
                    Version: kv.kvVersion[key],
                }
            }
        }
        
        // Serialize efficiently
        w := new(bytes.Buffer)
        e := labgob.NewEncoder(w)
        e.Encode(shardData)
        
        reply.State = w.Bytes()
        reply.Err = rpc.OK
        
        // Save the result
        kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
        return reply
    
    case shardrpc.InstallShardArgs:
        shard := cmd.Shard
        reply := shardrpc.InstallShardReply{}
        
        // Check for duplicate request
        clientId := cmd.ClientId
        requestId := cmd.RequestId
        if lastOp, ok := kv.clientTable[clientId]; ok && lastOp.RequestId == requestId {
            return lastOp.Response
        }
        
        // Update max config num
        if kv.maxShardNums[shard] < cmd.Num {
            kv.maxShardNums[shard] = cmd.Num
        } else if kv.maxShardNums[shard] > cmd.Num {
            reply.Err = rpc.ErrMaybe
            kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
            return reply
        }
        
        // Mark shard as owned and not frozen
        kv.ownedShards[shard] = true
        kv.frozenShards[shard] = false
        
        // Install state if provided
        if cmd.State != nil && len(cmd.State) > 0 {
            r := bytes.NewBuffer(cmd.State)
            d := labgob.NewDecoder(r)
            
            var shardData map[string]KeyValue
            if d.Decode(&shardData) != nil {
                reply.Err = rpc.ErrMaybe
                kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
                return reply
            }
            
            // Install the data
            for key, data := range shardData {
                kv.kvStore[key] = data.Value
                kv.kvVersion[key] = data.Version
            }
        }
        
        reply.Err = rpc.OK
        kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
        return reply
    
    case shardrpc.DeleteShardArgs:
        shard := cmd.Shard
        reply := shardrpc.DeleteShardReply{}
        
        // Check for duplicate request
        clientId := cmd.ClientId
        requestId := cmd.RequestId
        if lastOp, ok := kv.clientTable[clientId]; ok && lastOp.RequestId == requestId {
            return lastOp.Response
        }
        
        // Update max config num
        if kv.maxShardNums[shard] < cmd.Num {
            kv.maxShardNums[shard] = cmd.Num
        } else if kv.maxShardNums[shard] > cmd.Num {
            reply.Err = rpc.ErrMaybe
            kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
            return reply
        }
        
        // Only delete if we own this shard
        if kv.ownedShards[shard] {
            // Efficiently remove keys for this shard
            keysToDelete := []string{}
            for key := range kv.kvStore {
                if shardcfg.Key2Shard(key) == shard {
                    keysToDelete = append(keysToDelete, key)
                }
            }
            
            for _, key := range keysToDelete {
                delete(kv.kvStore, key)
                delete(kv.kvVersion, key)
            }
            
            // Mark shard as not owned
            delete(kv.ownedShards, shard)
            delete(kv.frozenShards, shard)
        }
        
        reply.Err = rpc.OK
        kv.clientTable[clientId] = OperationContext{RequestId: requestId, Response: reply}
        return reply
    }


	return nil
}


func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
    defer kv.mu.Unlock()
    
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    
    // Only include keys for shards we own - major memory savings
    ownedKVs := make(map[string]KeyValue)
    
    for key, value := range kv.kvStore {
        shard := shardcfg.Key2Shard(key)
        if kv.ownedShards[shard] {
            ownedKVs[key] = KeyValue{
                Value:   value,
                Version: kv.kvVersion[key],
            }
        }
    }
    
    // We don't need to store separate kvStore and kvVersion maps
    // Just store the combined version which saves space
    e.Encode(ownedKVs)
    
    // Only store essential state tracking info
    e.Encode(kv.clientTable)
    e.Encode(kv.ownedShards)
    e.Encode(kv.maxShardNums)
    
    return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }
    
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    
    var ownedKVs map[string]KeyValue
    var clientTable map[int64]OperationContext
    var ownedShards map[shardcfg.Tshid]bool
    var maxShardNums map[shardcfg.Tshid]shardcfg.Tnum
    
    if d.Decode(&ownedKVs) != nil ||
       d.Decode(&clientTable) != nil ||
       d.Decode(&ownedShards) != nil ||
       d.Decode(&maxShardNums) != nil {
        // Error decoding
        return
    }
    
    kv.mu.Lock()
    defer kv.mu.Unlock()
    
    // Clear existing data first
    kv.kvStore = make(map[string]string)
    kv.kvVersion = make(map[string]rpc.Tversion)
    
    // Restore KV data from combined structure
    for k, data := range ownedKVs {
        kv.kvStore[k] = data.Value
        kv.kvVersion[k] = data.Version
    }
    
    kv.clientTable = clientTable
    kv.ownedShards = ownedShards
    kv.maxShardNums = maxShardNums
    
    // Initialize frozenShards based on ownership
    kv.frozenShards = make(map[shardcfg.Tshid]bool)
    for shard := range kv.ownedShards {
        kv.frozenShards[shard] = false
    }
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
    // Check if this server owns the shard for this key
    shard := shardcfg.Key2Shard(args.Key)
    kv.mu.Lock()
    owned := kv.ownedShards[shard]
    kv.mu.Unlock()
    
    if !owned {
        reply.Err = rpc.ErrWrongGroup
        return
    }
    
    // Proceed with normal Get operation
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
    // Check if this server owns the shard for this key
    shard := shardcfg.Key2Shard(args.Key)
    kv.mu.Lock()
    owned := kv.ownedShards[shard]
    kv.mu.Unlock()
    
    if !owned {
        reply.Err = rpc.ErrWrongGroup
        return
    }
    
    // Proceed with normal Put operation
    err, result := kv.rsm.Submit(*args)
    
    if err != rpc.OK {
        reply.Err = err
        return
    }
    
    r := result.(rpc.PutReply)
    reply.Err = r.Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
    kv.mu.Lock()
    maxNum, exists := kv.maxShardNums[args.Shard]
    if exists && maxNum >= args.Num {
        kv.mu.Unlock()
        reply.Err = rpc.ErrMaybe
        return
    }
    kv.mu.Unlock()
    
    err, result := kv.rsm.Submit(*args)
    
    if err != rpc.OK {
        reply.Err = err
        return
    }
    
    r := result.(shardrpc.FreezeShardReply)
    reply.State = r.State
    reply.Err = r.Err
}


// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
    maxNum, exists := kv.maxShardNums[args.Shard]
    if exists && maxNum >= args.Num {
        kv.mu.Unlock()
        reply.Err = rpc.ErrMaybe
        return
    }
    kv.mu.Unlock()
    
    // Run the operation through RSM
    err, result := kv.rsm.Submit(*args)
    
    if err != rpc.OK {
        reply.Err = err
        return
    }
    
    r := result.(shardrpc.InstallShardReply)
    reply.Err = r.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	kv.mu.Lock()
    maxNum, exists := kv.maxShardNums[args.Shard]
    if exists && maxNum >= args.Num {
        kv.mu.Unlock()
        reply.Err = rpc.ErrMaybe
        return
    }
    kv.mu.Unlock()
    
    // Run the operation through RSM
    err, result := kv.rsm.Submit(*args)
    
    if err != rpc.OK {
        reply.Err = err
        return
    }
    
    r := result.(shardrpc.DeleteShardReply)
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
    log.Printf("Group%d Server%d Killed", kv.gid, kv.me)

	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	labgob.Register(rpc.GetReply{})
    labgob.Register(rpc.PutReply{})
	labgob.Register(OperationContext{})
    labgob.Register(KeyValue{})
    labgob.Register(map[string]KeyValue{})

	kv := &KVServer{
        me:        me,
		gid:       gid, 
        kvStore:   make(map[string]string),
        kvVersion: make(map[string]rpc.Tversion),
		clientTable: make(map[int64]OperationContext),
		ownedShards: make(map[shardcfg.Tshid]bool),
    	maxShardNums: make(map[shardcfg.Tshid]shardcfg.Tnum),
		frozenShards: make(map[shardcfg.Tshid]bool),
    }


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	snapshot := persister.ReadSnapshot()
    if len(snapshot) > 0 {
        kv.Restore(snapshot)
    } else if gid == shardcfg.Gid1 {
        // Only initialize ownership if we don't have saved state
        // and this is the first group
        for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
            kv.ownedShards[shard] = true
            kv.frozenShards[shard] = false
        }
    }
    log.Printf("Group%d Server%d Start", kv.gid, kv.me)
	return []tester.IService{kv, kv.rsm.Raft()}
}
