package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
	//"github.com/gofiber/fiber/v2/middleware/requestid"
)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
	RequestId int
	Req any
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	dead int32
	lastApplied int
	waitingOps map[int]chan OpResult

	killCh chan struct{}
	shutdownCh   chan struct{}

	snapshotTrigger *time.Ticker
}

type OpResult struct {
	Err rpc.Err
	Result any
	ClientId int
	RequestId int
}
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waitingOps:   make(map[int]chan OpResult),
        killCh:       make(chan struct{}),
		shutdownCh:   make(chan struct{}),
        lastApplied:  0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	snapshot := persister.ReadSnapshot()
    if len(snapshot) > 0 {
        sm.Restore(snapshot)
    }

	go rsm.applier()

	if maxraftstate != -1 {
        rsm.snapshotTrigger = time.NewTicker(100 * time.Millisecond)
        go rsm.snapshotMonitor()
    }

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) killed() bool {
    return atomic.LoadInt32(&rsm.dead) == 1
}

// 关闭RSM
func (rsm *RSM) Kill() {
    atomic.StoreInt32(&rsm.dead, 1)
    
    // 通知applier goroutine退出
    close(rsm.killCh)

	rsm.mu.Lock()
    if rsm.shutdownCh != nil {
        close(rsm.shutdownCh)
        rsm.shutdownCh = nil
    }

	if rsm.snapshotTrigger != nil {
        rsm.snapshotTrigger.Stop()
    }

	for idx, ch := range rsm.waitingOps {
        select {
        case ch <- OpResult{Err: rpc.ErrWrongLeader}:
        default:
        }
        delete(rsm.waitingOps, idx)
    }
    rsm.mu.Unlock()
    
    if rsm.rf != nil {
        rsm.rf.Kill()
    }
}

func (rsm *RSM) applier() {
	for {
		select {
		case <-rsm.killCh :
			return
		case msg, ok := <-rsm.applyCh:
			if !ok {
				return
			}

			if msg.CommandValid {
				rsm.mu.Lock()

				if msg.CommandIndex <= rsm.lastApplied {
					rsm.mu.Unlock()
					continue
				}

				rsm.lastApplied = msg.CommandIndex

				op, ok := msg.Command.(Op)
				if !ok {
					rsm.mu.Unlock()
					continue
				}

				result := rsm.sm.DoOp(op.Req)

				opResult := OpResult{
                    Err:       rpc.OK,
                    Result:    result,
                    ClientId:  op.ClientId,
                    RequestId: op.RequestId,
                }

				ch, waiting := rsm.waitingOps[msg.CommandIndex]

				if waiting {
					select {
                    case ch <- opResult:
                    default:
					}
				}

				if rsm.maxraftstate > 0 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
					snapshot := rsm.sm.Snapshot()
					rsm.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				rsm.mu.Unlock()
			} else if msg.SnapshotValid {
				rsm.mu.Lock()
                
                if msg.SnapshotIndex <= rsm.lastApplied {
                    rsm.mu.Unlock()
                    continue
                }
                
                rsm.sm.Restore(msg.Snapshot)
                rsm.lastApplied = msg.SnapshotIndex
                
                for idx, ch := range rsm.waitingOps {
                    if idx <= msg.SnapshotIndex {
                        select {
                        case ch <- OpResult{Err: rpc.ErrWrongLeader}:
                        default:
                        }
                        delete(rsm.waitingOps, idx)
                    }
                }
                
                rsm.mu.Unlock()
			}
		}
	}
}

func (rsm *RSM) snapshotMonitor() {
    for !rsm.killed() {
        <-rsm.snapshotTrigger.C
        
        rsm.mu.Lock()
        
        if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate * 9/10 {
            snapshot := rsm.sm.Snapshot()
            rsm.rf.Snapshot(rsm.lastApplied, snapshot)
        }
        
        rsm.mu.Unlock()
    }
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	if rsm.killed() {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	clientId := rsm.me
	requestId := int(time.Now().UnixNano())

	op := Op {
		ClientId:  clientId,
        RequestId: requestId,
        Req:       req,
	}

	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
        return rpc.ErrWrongLeader, nil
    }

	resultCh := make(chan OpResult, 1)

	rsm.mu.Lock()
	shutdownCh := rsm.shutdownCh
	rsm.waitingOps[index] = resultCh
	rsm.mu.Unlock()

	checkTicker := time.NewTicker(50 * time.Millisecond)
    defer checkTicker.Stop()

	TimeoutCh := time.After(5 * time.Second)
    
    for {
        select {
        case result := <-resultCh:
            rsm.mu.Lock()
            delete(rsm.waitingOps, index)
            rsm.mu.Unlock()
            
            if result.ClientId == clientId && result.RequestId == requestId {
                return result.Err, result.Result
            } else {
                return rpc.ErrWrongLeader, nil
            }
            
        case <-checkTicker.C:
			if rsm.killed() {
                rsm.mu.Lock()
                delete(rsm.waitingOps, index)
                rsm.mu.Unlock()
                return rpc.ErrWrongLeader, nil
            }

            currentTerm, isStillLeader := rsm.rf.GetState()
            
            if term != currentTerm || !isStillLeader {
                rsm.mu.Lock()
                delete(rsm.waitingOps, index)
                rsm.mu.Unlock()
                return rpc.ErrWrongLeader, nil
            }

		case <-shutdownCh:
            rsm.mu.Lock()
            delete(rsm.waitingOps, index)
            rsm.mu.Unlock()
            return rpc.ErrWrongLeader, nil

		case <-TimeoutCh:
            rsm.mu.Lock()
            delete(rsm.waitingOps, index)
            rsm.mu.Unlock()
            return rpc.ErrWrongLeader, nil
        }
    }
}
