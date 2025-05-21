package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//"6.5840/kvsrv1/lock"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state ServerState

	curTerm  int
	votedFor int
	log      []*LogEntry

	commitIdx   int
	lastApplied int

	nextIdx  []int
	matchIdx []int

	lastHeartbeat time.Time

	applyCh     chan raftapi.ApplyMsg
	snapshotMsg raftapi.ApplyMsg

	lastIncludedTerm int
	lastIncludedIdx  int
}

type LogEntry struct {
	Idx     int
	Term    int
	Command any
}

type ServerState int

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIdx)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)
	//rf.persister.Save(raftstate, nil)
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIdx)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curterm int
	var votedFor int
	var log []*LogEntry
	var lastIncludedIdx int
	var lastIncludedTerm int
	if d.Decode(&curterm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIdx) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	} else {
		rf.curTerm = curterm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIdx = lastIncludedIdx
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = max(rf.lastApplied, lastIncludedIdx)
		rf.commitIdx = max(rf.commitIdx, lastIncludedIdx)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) getByIndex(index int) (int, bool) {
	i, j := 0, len(rf.log)-1
	for i < j {
		mid := (i + j) >> 1
		if rf.log[mid].Idx >= index {
			j = mid
		} else {
			i = mid + 1
		}
	}
	//i := index - rf.lastIncludedIdx
	return i, i >= 0 && i < len(rf.log) && rf.log[i].Idx == index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIdx || rf.commitIdx < index {
		return
	}
	k, _ := rf.getByIndex(index)
	rf.lastIncludedTerm = rf.log[k].Term
	rf.lastIncludedIdx = rf.log[k].Idx
	rf.log = rf.log[k:]
	rf.log[0].Idx = rf.lastIncludedIdx

	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIdx)
	rf.persistSnapshot(snapshot)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedIdx  int
	LastIncludedTerm int

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    
    reply.Term = rf.curTerm
    
    if args.Term < rf.curTerm {
        rf.mu.Unlock()
        return
    }
    
    rf.lastHeartbeat = time.Now()
    
    if args.Term > rf.curTerm {
        rf.curTerm = args.Term
        rf.votedFor = -1
        rf.state = FOLLOWER
        rf.persist()
    }
    
    rf.log = make([]*LogEntry, 1)
    rf.log[0] = &LogEntry{Idx: args.LastIncludedIdx, Term: args.LastIncludedTerm}
    
    rf.commitIdx = max(rf.commitIdx, args.LastIncludedIdx)
    //rf.lastApplied = max(rf.lastApplied, args.LastIncludedIdx)
    rf.lastIncludedIdx = args.LastIncludedIdx
    rf.lastIncludedTerm = args.LastIncludedTerm
    
    rf.persist()
    
    msg := raftapi.ApplyMsg{
        Snapshot:      args.Data,
        SnapshotValid: true,
        SnapshotIndex: args.LastIncludedIdx,
        SnapshotTerm:  args.LastIncludedTerm,
    }

	rf.snapshotMsg = msg
    
    rf.mu.Unlock()
    
    rf.Snapshot(args.LastIncludedIdx, args.Data)
    //rf.applyCh <- msg
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

func isLogNew(cLastIdx, cLastTerm, rLastIdx, rLastTerm int) bool {
	return (cLastTerm > rLastTerm) || (cLastTerm == rLastTerm && cLastIdx >= rLastIdx)
}

func (rf *Raft) LastLogIdx() int {
	return rf.log[len(rf.log)-1].Idx
}

func (rf *Raft) LastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.curTerm

	if args.Term <= rf.curTerm {
		reply.VoteGranted = false
		return
	}

	rf.curTerm = args.Term
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.persist()

	vote := (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		isLogNew(args.LastLogIdx, args.LastLogTerm, rf.LastLogIdx(), rf.LastLogTerm())

	if vote {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}

	// Your code here (3A, 3B).
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArg struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// opt
	Conflict bool
	XTerm    int
	XIdx     int
	XLen     int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.curTerm

	if args.Term < rf.curTerm {
		return
	}

	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}

	rf.lastHeartbeat = time.Now()

	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
		rf.persist()
	}

	if rf.LastLogIdx() < args.PrevLogIdx {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIdx = -1
		reply.XLen = rf.LastLogIdx() + 1
		return
	}

	k, _ := rf.getByIndex(args.PrevLogIdx)

	if rf.log[k].Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log[k].Term
		for xIndex := k; xIndex > 0; xIndex-- {
			if rf.log[xIndex-1].Term != xTerm {
				reply.XIdx = rf.log[xIndex].Idx
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.LastLogIdx() + 1
		return
	}

	size := len(args.Entries)
	for i := 1; i <= size; i++ {
		if i+k < len(rf.log) && rf.log[i+k].Term != args.Entries[i-1].Term {
			rf.log = rf.log[:i+k]
		}
		if len(rf.log) <= i+k {
			rf.log = append(rf.log, args.Entries[i-1])
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = min(args.LeaderCommit, rf.LastLogIdx())
		//rf.applyCommitedEntries()
	}

	reply.Success = true
}

func (rf *Raft) applyCommitedEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if len(rf.snapshotMsg.Snapshot) > 0 {
			msg := raftapi.ApplyMsg{
				CommandValid:  false,
				Snapshot:      rf.snapshotMsg.Snapshot,
				SnapshotValid: true,
				SnapshotIndex: rf.snapshotMsg.SnapshotIndex,
				SnapshotTerm:  rf.snapshotMsg.SnapshotTerm,
			}
			rf.snapshotMsg = raftapi.ApplyMsg{}
			rf.mu.Unlock()

			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			msgsToApply := make([]raftapi.ApplyMsg, 0, rf.commitIdx-rf.lastApplied)
			if rf.commitIdx > rf.lastApplied {
				for i := rf.lastApplied + 1; i <= rf.commitIdx; i++ {
					rf.lastApplied = i
					index, _ := rf.getByIndex(rf.lastApplied)

					if rf.lastApplied <= rf.lastIncludedIdx {
						continue
					}

					msg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.log[index].Command,
						CommandIndex: i,
						SnapshotTerm: rf.log[index].Term,
					}
					msgsToApply = append(msgsToApply, msg)
				}

			}
			rf.mu.Unlock()

			for _, msg := range msgsToApply {
				rf.applyCh <- msg				
			}
			rf.mu.Lock()
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.state = CANDIDATE
	rf.curTerm++
	rf.votedFor = rf.me
	curterm := rf.curTerm
	lastlogidx := rf.LastLogIdx()
	lastlogterm := rf.LastLogTerm()
	rf.lastHeartbeat = time.Now()
	rf.persist()
	rf.mu.Unlock()

	votes := 1
	votesneed := len(rf.peers)/2 + 1

	var votemu sync.Mutex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:        curterm,
				CandidateId: rf.me,
				LastLogIdx:  lastlogidx,
				LastLogTerm: lastlogterm,
			}
			reply := &RequestVoteReply{}

			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.curTerm {
					rf.curTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					return
				}

				if rf.state == CANDIDATE && rf.curTerm == args.Term && reply.VoteGranted {
					votemu.Lock()
					votes++
					if rf.state == CANDIDATE && votes >= votesneed {
						rf.state = LEADER
						for i := range rf.peers {
							rf.nextIdx[i] = len(rf.log) + rf.lastIncludedIdx
							//rf.nextIdx[i] = lastlogidx + 1
							rf.matchIdx[i] = rf.lastIncludedIdx
						}
						rf.persist()
						go rf.sendHeartbeat()
					}
					votemu.Unlock()
				}
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	term := rf.curTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			rf.mu.Lock()
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER || rf.curTerm != term {
				rf.mu.Unlock()
				return
			}

			if rf.nextIdx[server] <= rf.lastIncludedIdx {
				args := InstallSnapshotArgs{}
				reply := InstallSnapshotReply{}
				args.Term = rf.curTerm
				args.LeaderId = rf.me
				args.LastIncludedTerm = rf.lastIncludedTerm
				args.LastIncludedIdx = rf.lastIncludedIdx
				args.Data = rf.persister.ReadSnapshot()
				rf.mu.Unlock()

				if rf.sendInstallSnapshot(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.curTerm {
						rf.curTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.persist()
						return
					}
					if rf.curTerm != args.Term {
						return
					}
					rf.nextIdx[server] = max(rf.nextIdx[server], args.LastIncludedIdx) + 1
					rf.matchIdx[server] = rf.nextIdx[server] - 1
					rf.updateCommitIdx()
				}

			} else {
				prevLogIdx := rf.nextIdx[server] - 1
				var prevLogTerm int

				if prevLogIdx == rf.lastIncludedIdx {
					prevLogTerm = rf.lastIncludedTerm
				} else {
					localIdx := prevLogIdx - rf.lastIncludedIdx
					prevLogTerm = rf.log[localIdx].Term
				}

				// 添加需要复制的日志条目
				entries := make([]*LogEntry, 0)
				if rf.nextIdx[server] <= rf.LastLogIdx() {
					//startIdx := rf.nextIdx[server] - rf.lastIncludedIdx
					startIdx, _ := rf.getByIndex(rf.nextIdx[server])
					entries = append(entries, rf.log[startIdx:]...)
				}

				args := &AppendEntriesArg{
					Term:         rf.curTerm,
					LeaderId:     rf.me,
					PrevLogIdx:   prevLogIdx,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIdx,
				}

				reply := &AppendEntriesReply{}

				rf.mu.Unlock()

				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.curTerm {
						rf.curTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.persist()
						return
					}

					if rf.state == LEADER && rf.curTerm == term {
						if reply.Success {
							newNextIdx := args.PrevLogIdx + 1 + len(args.Entries)
							newMatchIdx := newNextIdx - 1

							rf.nextIdx[server] = max(rf.nextIdx[server], newNextIdx)
							rf.matchIdx[server] = max(rf.matchIdx[server], newMatchIdx)
							rf.updateCommitIdx()
						} else if reply.Conflict {
							if reply.XTerm == -1 {
								rf.nextIdx[server] = reply.XLen
							} else {
								found := false
								for i := len(rf.log) - 1; i >= 0; i-- {
									if rf.log[i].Term == reply.XTerm {
										// 找到了该任期的日志，将nextIndex设置为该任期的最后一个条目之后
										rf.nextIdx[server] = rf.log[i].Term + 1
										found = true
										break
									}
								}

								if !found {
									// 未找到该任期，直接跳到follower中该任期的第一个条目
									rf.nextIdx[server] = reply.XIdx
								}
							}
						} else {
							if rf.nextIdx[server] > 1 {
								rf.nextIdx[server]--
							}
						}
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) updateCommitIdx() {
	for N := rf.commitIdx + 1; N <= rf.LastLogIdx(); N++ {
		n, found := rf.getByIndex(N)
		if !found {
			continue
		}

		if rf.log[n].Term == rf.curTerm {
			cnt := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIdx[i] >= n {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIdx = N
				//rf.applyCommitedEntries()
			} else {
				break
			}
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}
	index := rf.LastLogIdx() + 1
	term := rf.curTerm

	log := &LogEntry{
		Idx:     index,
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, log)
	rf.persist()
	rf.nextIdx[rf.me] = index
	rf.matchIdx[rf.me] = index - 1

	//rf.applyCommitedEntries()

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		timeoutms := 200 + (rand.Int() % 200)
		timeout := time.Since(rf.lastHeartbeat) > time.Duration(timeoutms)*time.Millisecond
		state := rf.state
		rf.mu.Unlock()
		if timeout && (state == FOLLOWER || state == CANDIDATE) {
			rf.startElection()
		} else if state == LEADER {
			rf.sendHeartbeat()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C)
	// 初始化Raft状态
	rf.curTerm = 0
	rf.votedFor = -1
	rf.log = make([]*LogEntry, 0)
	// 添加一个空日志条目在索引0，简化索引操作
	rf.log = append(rf.log, &LogEntry{Idx: 0, Term: 0})

	// 初始化volatile状态
	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	rf.lastIncludedIdx = 0
	rf.lastIncludedTerm = 0

	// 为leader初始化状态
	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))

	// 初始化选举超时相关变量
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommitedEntries()

	return rf
}
