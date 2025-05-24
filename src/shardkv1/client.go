package shardkv

import (
    "sync"
    "time"

    "6.5840/kvsrv1/rpc"
    "6.5840/shardkv1/shardcfg"
    "6.5840/shardkv1/shardgrp"
    "6.5840/tester1"
)

type Clerk struct {
    clnt      *tester.Clnt
    sck       ShardCtrlerQuery // interface for querying shard controller
    
    mu        sync.Mutex
    config    *shardcfg.ShardConfig
    grpClerks map[tester.Tgid]*shardgrp.Clerk
}

// Interface for querying the shard controller
type ShardCtrlerQuery interface {
    Query() *shardcfg.ShardConfig
}

func MakeClerk(clnt *tester.Clnt, sck ShardCtrlerQuery) *Clerk {
    ck := &Clerk{
        clnt:      clnt,
        sck:       sck,
        config:    sck.Query(),
        grpClerks: make(map[tester.Tgid]*shardgrp.Clerk),
    }
    return ck
}

// Get the clerk for a specific shardgroup, creating it if necessary
func (ck *Clerk) getShardGrpClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
    ck.mu.Lock()
    defer ck.mu.Unlock()
    
    if clerk, exists := ck.grpClerks[gid]; exists {
        return clerk
    }
    
    clerk := shardgrp.MakeClerk(ck.clnt, servers)
    ck.grpClerks[gid] = clerk
    return clerk
}

// Update configuration when needed
func (ck *Clerk) updateConfig() {
    ck.mu.Lock()
    defer ck.mu.Unlock()
    
    newConfig := ck.sck.Query()
    if newConfig != nil && (ck.config == nil || newConfig.Num > ck.config.Num) {
        ck.config = newConfig
    }
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
    maxRetry := 10

    for retry := 0; retry < maxRetry; retry++  {
        ck.mu.Lock()
        config := ck.config
        ck.mu.Unlock()
        
        if config == nil {
            ck.updateConfig()
            time.Sleep(100 * time.Millisecond)
            continue
        }
        
        shard := shardcfg.Key2Shard(key)
        gid, servers, ok := config.GidServers(shard)
        
        if !ok || gid == 0 || len(servers) == 0 {
            ck.updateConfig()
            time.Sleep(100 * time.Millisecond)
            continue
        }
        
        // Get or create the clerk for this group
        grpClerk := ck.getShardGrpClerk(gid, servers)
        value, version, err := grpClerk.Get(key)
        
        if err == rpc.ErrWrongGroup {
            // Group doesn't believe it owns this shard, config may be stale
            ck.updateConfig()
            time.Sleep(100 * time.Millisecond)
            continue
        }
        
        return value, version, err
    }

    return "", 0, rpc.ErrMaybe
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
    maxRetry := 10
    
    for retry := 0; retry < maxRetry; retry++ {
        ck.mu.Lock()
        config := ck.config
        ck.mu.Unlock()
        
        if config == nil {
            ck.updateConfig()
            time.Sleep(100 * time.Millisecond)
            continue
        }
        
        shard := shardcfg.Key2Shard(key)
        gid, servers, ok := config.GidServers(shard)
        
        if !ok || gid == 0 || len(servers) == 0 {
            ck.updateConfig()
            time.Sleep(100 * time.Millisecond)
            continue
        }
        
        // Get or create the clerk for this group
        grpClerk := ck.getShardGrpClerk(gid, servers)
        err := grpClerk.Put(key, value, version)
        
        if err == rpc.ErrWrongGroup {
            // Group doesn't believe it owns this shard, config may be stale
            ck.updateConfig()
            time.Sleep(100 * time.Millisecond)
            continue
        } else if err == rpc.ErrVersion {
            return rpc.ErrMaybe
        }
        
        return err
    }
    return rpc.ErrMaybe
}