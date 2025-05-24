package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
    clnt    *tester.Clnt
    servers []string
    // You will have to modify this struct.
    mu sync.Mutex
    leaderId int
    clientId int64
    requestId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
    ck := &Clerk{
        clnt:      clnt,
        servers:   servers,
        leaderId:  0,
        clientId:  time.Now().UnixNano(), // Use timestamp as unique client ID
        requestId: 0,
    }
    // You'll have to add code here.
    return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
    for maxRetry := 5; maxRetry > 0 ; maxRetry-- {
        ck.mu.Lock()
        requestId := ck.requestId
        ck.requestId++
        serverId := ck.leaderId
        ck.mu.Unlock()

        args := &rpc.GetArgs{
            Key:       key,
            ClientId:  ck.clientId,
            RequestId: requestId,
        }
        
        for i := 0; i < len(ck.servers); i++ {
            reply := &rpc.GetReply{}
            ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", args, reply)
            if ok && reply.Err != rpc.ErrWrongLeader {
                ck.mu.Lock()
                ck.leaderId = serverId
                ck.mu.Unlock()
                return reply.Value, reply.Version, reply.Err
            }
            serverId = (serverId + 1) % len(ck.servers)
            time.Sleep(10 * time.Millisecond)
        }

        time.Sleep(100* time.Millisecond)
    }
    
    
    return "", 0, rpc.ErrWrongGroup
}


func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
    ck.mu.Lock()
    requestId := ck.requestId
    ck.requestId++
    serverId := ck.leaderId
    ck.mu.Unlock()

    args := &rpc.PutArgs{
        Key:       key,
        Value:     value,
        Version:   version,
        ClientId:  ck.clientId,
        RequestId: requestId,
    }

    firstloop := true

    for i := 0; i < len(ck.servers)*3; i++ {
        reply := &rpc.PutReply{}
        ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", args, reply)
        if ok && reply.Err != rpc.ErrWrongLeader {
            ck.mu.Lock()
            ck.leaderId = serverId
            ck.mu.Unlock()
            if reply.Err == rpc.ErrVersion && !firstloop {
                return rpc.ErrMaybe
            }
            return reply.Err
        }
        firstloop = false
        serverId = (serverId + 1) % len(ck.servers)
        time.Sleep(10 * time.Millisecond)
    }
    
    // If we've tried all servers and still no success, return ErrWrongGroup
    return rpc.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
    ck.mu.Lock()
    requestId := ck.requestId
    ck.requestId++
    serverId := ck.leaderId
    ck.mu.Unlock()

    args := &shardrpc.FreezeShardArgs{
        Shard:     s,
        Num:       num,
        ClientId:  ck.clientId,
        RequestId: requestId,
    }

    for i := 0; i < len(ck.servers); i++ {
        reply := &shardrpc.FreezeShardReply{}
        ok := ck.clnt.Call(ck.servers[serverId], "KVServer.FreezeShard", args, reply)
        if ok && reply.Err != rpc.ErrWrongLeader {
            ck.mu.Lock()
            ck.leaderId = serverId
            ck.mu.Unlock()
            return reply.State, reply.Err
        }
        serverId = (serverId + 1) % len(ck.servers)
        time.Sleep(100 * time.Millisecond)
    }
    
    return nil, rpc.ErrWrongGroup
}


func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
    // Your code here
    ck.mu.Lock()
    requestId := ck.requestId
    ck.requestId++
    serverId := ck.leaderId
    ck.mu.Unlock()

    args := &shardrpc.InstallShardArgs{
        Shard: s,
        State: state,
        Num: num,
        ClientId: ck.clientId,
        RequestId: requestId,
    }

    for i := 0; i < len(ck.servers)*3; i++ { // Try harder for configuration operations
        reply := &shardrpc.InstallShardReply{}
        ok := ck.clnt.Call(ck.servers[serverId], "KVServer.InstallShard", args, reply)
        if ok && reply.Err != rpc.ErrWrongLeader {
            ck.mu.Lock()
            ck.leaderId = serverId
            ck.mu.Unlock()
            return reply.Err
        }
        serverId = (serverId + 1) % len(ck.servers)
        time.Sleep(10 * time.Millisecond)
    }
    
    return rpc.ErrWrongGroup
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
    // Your code here
    ck.mu.Lock()
    requestId := ck.requestId
    ck.requestId++
    serverId := ck.leaderId
    ck.mu.Unlock()

    args := &shardrpc.DeleteShardArgs{
        Shard: s,
        Num: num,
        ClientId: ck.clientId,
        RequestId: requestId,
    }

    for i := 0; i < len(ck.servers)*3; i++ { // Try harder for configuration operations
        reply := &shardrpc.DeleteShardReply{}
        ok := ck.clnt.Call(ck.servers[serverId], "KVServer.DeleteShard", args, reply)
        if ok && reply.Err != rpc.ErrWrongLeader {
            ck.mu.Lock()
            ck.leaderId = serverId
            ck.mu.Unlock()
            return reply.Err
        }
        serverId = (serverId + 1) % len(ck.servers)
        time.Sleep(10 * time.Millisecond)
    }
    
    return rpc.ErrWrongGroup
}
