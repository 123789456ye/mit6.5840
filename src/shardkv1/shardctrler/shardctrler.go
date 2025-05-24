package shardctrler

import (
	"fmt"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
    clnt *tester.Clnt
    kvtest.IKVClerk

    killed int32 // set by Kill()

    // Your data here.
    grpClerks map[tester.Tgid]*shardgrp.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
    sck := &ShardCtrler{clnt: clnt}
    srv := tester.ServerName(tester.GRP0, 0)
    sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
    // Your code here.
    sck.grpClerks = make(map[tester.Tgid]*shardgrp.Clerk)
    return sck
}

// The tester calls InitController() before starting a new
// controller. For Part C, this method implements recovery by checking
// if there's an interrupted reconfiguration to complete.
func (sck *ShardCtrler) InitController() {
    // Check if there's a next configuration
    nextConfigStr, nextVersion, err := sck.Get("next_config")
    if err != rpc.OK || nextConfigStr == "" {
        return // No next configuration
    }
    
    nextConfig := shardcfg.FromString(nextConfigStr)
    if nextConfig == nil {
        return // Invalid next configuration
    }
    
    // Check if current configuration exists
    currentConfig := sck.Query()
    if currentConfig == nil || nextConfig.Num > currentConfig.Num {
        // Need to complete the reconfiguration
        if currentConfig != nil {
            // Process any pending shard movements
            for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
                oldGid := currentConfig.Shards[shard]
                newGid := nextConfig.Shards[shard]
                
                if oldGid != newGid {
                    if oldGid == 0 {
                        // Shard was previously unassigned
                        if newGid != 0 {
                            sck.installEmptyShard(shard, newGid, nextConfig)
                        }
                    } else if newGid == 0 {
                        // Shard is being unassigned
                        sck.removeShardFromGroup(shard, oldGid, currentConfig, nextConfig)
                    } else {
                        // Moving from one group to another
                        sck.moveShardBetweenGroups(shard, oldGid, newGid, currentConfig, nextConfig)
                    }
                }
            }
        }
        
        // Verify all shard movements are complete
        // For Part C, we might need to retry a few times to ensure everything is ready
        sck.verifyShardMovements(currentConfig, nextConfig)
        
        // Make next configuration the current one
        sck.Put("current_config", nextConfigStr, 0)
        
        // Delete next configuration with versioned write to avoid races
        sck.Put("next_config", "", nextVersion)
    }
}

// Verify all shard movements are complete
func (sck *ShardCtrler) verifyShardMovements(current, next *shardcfg.ShardConfig) {
    if current == nil || next == nil {
        return
    }
    
    maxRetries := 3
    
    for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
        oldGid := current.Shards[shard]
        newGid := next.Shards[shard]
        
        if oldGid != newGid && newGid != 0 {
            // Verify the new group has the shard
            servers, ok := next.Groups[newGid]
            if !ok || len(servers) == 0 {
                continue
            }
            
            clerk := sck.getGroupClerk(newGid, servers)
            
            // Try a few times to ensure the shard is installed
            for retry := 0; retry < maxRetries; retry++ {
                // We can use a dummy key from this shard to check if the group owns it
                dummyKey := fmt.Sprintf("Shared%d", shard)
                _, _, err := clerk.Get(dummyKey)
                
                if err != rpc.ErrWrongGroup {
                    // The group either has the key or reports another error, but not ErrWrongGroup
                    // This suggests it knows it owns the shard
                    break
                }
                
                // If we get ErrWrongGroup, the shard might not be installed yet
                // Try installing it again
                if oldGid == 0 {
                    sck.installEmptyShard(shard, newGid, next)
                } else {
                    // Try to move it again
                    sck.moveShardBetweenGroups(shard, oldGid, newGid, current, next)
                }
                
                time.Sleep(100 * time.Millisecond)
            }
        }
    }
}

// Called once by the tester to supply the first configuration.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
    configStr := cfg.String()
    sck.Put("current_config", configStr, 0)
}

// Get or create a clerk for a specific group
func (sck *ShardCtrler) getGroupClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
    if clerk, ok := sck.grpClerks[gid]; ok {
        return clerk
    }
    
    clerk := shardgrp.MakeClerk(sck.clnt, servers)
    sck.grpClerks[gid] = clerk
    return clerk
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
    current := sck.Query()
    if current == nil {
        sck.InitConfig(new)
        return
    }

    if current.Num == new.Num {
        return // Nothing to do
    }
    
    // Check if there's already a next configuration
    nextConfigStr, nextVersion, err := sck.Get("next_config")
    if err == rpc.OK && nextConfigStr != "" {
        nextConfig := shardcfg.FromString(nextConfigStr)
        if nextConfig != nil {
            if nextConfig.Num > new.Num {
                // A newer configuration is already being processed
                return
            } else if nextConfig.Num == new.Num {
                // Same configuration is already being processed
                // Check if it's identical
                if nextConfig.String() == new.String() {
                    // Same configuration, continue with the move
                } else {
                    // Different configuration with same number - this is a conflict
                    // For Part C, we'll respect the existing one
                    return
                }
            }
        }
    }
    
    // For Part C, we use two-phase approach: store next config first
    newConfigStr := new.String()
    if nextConfigStr == "" || nextVersion == 0 {
        // No existing next_config, create it
        err = sck.Put("next_config", newConfigStr, 0)
    } else {
        // Update existing next_config with versioned write
        err = sck.Put("next_config", newConfigStr, nextVersion)
    }
    
    if err != rpc.OK {
        // Failed to store next_config, another controller might be working
        return
    }
    
    // Now get the latest version for any future updates
    nextConfigStr, nextVersion, _ = sck.Get("next_config")
    
    // Process shard movements
    for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
        oldGid := current.Shards[shard]
        newGid := new.Shards[shard]
        
        if oldGid != newGid {
            if oldGid == 0 {
                // Shard was previously unassigned
                if newGid != 0 {
                    sck.installEmptyShard(shard, newGid, new)
                }
            } else if newGid == 0 {
                // Shard is being unassigned
                sck.removeShardFromGroup(shard, oldGid, current, new)
            } else {
                // Moving from one group to another
                sck.moveShardBetweenGroups(shard, oldGid, newGid, current, new)
            }
        }
    }
    
    // For Part C, verify movements are complete
    sck.verifyShardMovements(current, new)
    
    // Now that all movements are done, update the current configuration
    _, currentVersion, _ := sck.Get("current_config")
    err = sck.Put("current_config", newConfigStr, currentVersion)
    
    if err != rpc.OK {
        // Failed to update current config, another controller might have done it
        // We should check if our config was applied
        latestConfig := sck.Query()
        if latestConfig == nil || latestConfig.Num < new.Num {
            // Our config wasn't applied, retry the Put
            sck.Put("current_config", newConfigStr, 0)
        }
    }
    
    // Finally, clear the next_config to signal we're done
    // Get the latest version first to avoid race conditions
    _, nextVersion, _ = sck.Get("next_config")
    sck.Put("next_config", "", nextVersion)
}

// Install empty shard to a newly assigned group
func (sck *ShardCtrler) installEmptyShard(shard shardcfg.Tshid, gid tester.Tgid, cfg *shardcfg.ShardConfig) {
    servers, ok := cfg.Groups[gid]
    if !ok || len(servers) == 0 {
        return
    }
    
    clerk := sck.getGroupClerk(gid, servers)
    // For Part C, try a few times to ensure it succeeds
    for retry := 0; retry < 3; retry++ {
        err := clerk.InstallShard(shard, nil, cfg.Num)
        if err == "" {
            // Success
            return
        }
        time.Sleep(100 * time.Millisecond)
    }
}

// Remove shard from a group that's no longer responsible for it
func (sck *ShardCtrler) removeShardFromGroup(shard shardcfg.Tshid, gid tester.Tgid, current, new *shardcfg.ShardConfig) {
    servers, ok := current.Groups[gid]
    if !ok || len(servers) == 0 {
        return
    }
    
    clerk := sck.getGroupClerk(gid, servers)
    
    // First freeze the shard
    var freezeErr rpc.Err
    
    // For Part C, try a few times to ensure it succeeds
    for retry := 0; retry < 3; retry++ {
        _, freezeErr = clerk.FreezeShard(shard, new.Num)
        if freezeErr == rpc.OK {
            // Success
            break
        }
        time.Sleep(100 * time.Millisecond)
    }
    
    // If we couldn't freeze after retries, we can still try to delete
    
    // Then delete the shard
    for retry := 0; retry < 3; retry++ {
        err := clerk.DeleteShard(shard, new.Num)
        if err == "" {
            // Success
            return
        }
        time.Sleep(100 * time.Millisecond)
    }
}

// Move shard from one group to another
func (sck *ShardCtrler) moveShardBetweenGroups(shard shardcfg.Tshid, oldGid, newGid tester.Tgid, current, new *shardcfg.ShardConfig) {
    // Get servers for source group
    oldServers, oldOk := current.Groups[oldGid]
    if !oldOk || len(oldServers) == 0 {
        return
    }
    
    // Get servers for destination group
    newServers, newOk := new.Groups[newGid]
    if !newOk || len(newServers) == 0 {
        return
    }
    
    // Get clerks for both groups
    oldClerk := sck.getGroupClerk(oldGid, oldServers)
    newClerk := sck.getGroupClerk(newGid, newServers)
    
    // Step 1: Freeze the shard at source group
    var shardState []byte
    var freezeErr rpc.Err
    
    // For Part C, try a few times to ensure it succeeds
    for retry := 0; retry < 3; retry++ {
        shardState, freezeErr = oldClerk.FreezeShard(shard, new.Num)
        if freezeErr == rpc.OK {
            // Success
            break
        }
        time.Sleep(100 * time.Millisecond)
    }
    
    if freezeErr != "" {
        // Failed to freeze after retries
        return
    }
    
    // Step 2: Install the shard at destination group
    for retry := 0; retry < 3; retry++ {
        err := newClerk.InstallShard(shard, shardState, new.Num)
        if err == "" {
            // Success
            break
        }
        time.Sleep(100 * time.Millisecond)
    }
    
    // Step 3: Delete the shard from source group
    // Even if install failed, we try to delete for cleanup
    for retry := 0; retry < 3; retry++ {
        err := oldClerk.DeleteShard(shard, new.Num)
        if err == "" {
            // Success
            return
        }
        time.Sleep(100 * time.Millisecond)
    }
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
    configStr, _, err := sck.Get("current_config")
    if err != rpc.OK || configStr == "" {
        return nil
    }
    return shardcfg.FromString(configStr)
}
