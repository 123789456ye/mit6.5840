package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const MAX_TASK_TIME = 10

const  (
	TODO_STATUE int = iota
	PROC_STATUE
	COMP_STATUE
)

const (
	MAP_TASK int = iota
	RED_TASK
)

const (
	MED_FORMAT = "mr-%v-%v"
	OUT_FORMAT = "mr-out-%v"
)

type Coordinator struct {
	nMap int
	nReduce int
	inputFiles []string

	mLock sync.Mutex
	mTasks []int
	mCnt int
	mExpire []int64
	mtodo int

	rLock sync.Mutex
	rTasks []int
	rCnt int
	rExpire []int64
	rtodo int
}

func (c *Coordinator) GetTask(req *GetTaskReq, rsp *GetTaskRsp) error {
	rsp.NMap = c.nMap
	rsp.NReduce = c.nReduce
	rsp.TaskSeqNum = -1
	c.mLock.Lock()
	if c.mCnt < c.nMap {
		/*for i, s := range c.mTasks {
			if s != TODO_STATUE {continue}
			c.mTasks[i] = PROC_STATUE
			c.mExpire[i] = time.Now().Unix() + MAX_TASK_TIME
			rsp.InputFileName = c.inputFiles[i]
			rsp.TaskSeqNum = i
			rsp.TaskType = MAP_TASK
			break
		}*/
		start := c.mtodo
		for {
			if c.mTasks[c.mtodo] == TODO_STATUE {
				c.mTasks[c.mtodo] = PROC_STATUE
				c.mExpire[c.mtodo] = time.Now().Unix() + MAX_TASK_TIME
				rsp.InputFileName = c.inputFiles[c.mtodo]
				rsp.TaskSeqNum = c.mtodo
				rsp.TaskType = MAP_TASK

				c.mtodo += 1
				if c.mtodo >= c.nMap {
					c.mtodo -= c.nMap
				}
				break
			}
			c.mtodo += 1 
			if c.mtodo >= c.nMap {
				c.mtodo -= c.nMap
			}
			if c.mtodo == start {
				break
			}
		}
		c.mLock.Unlock()
		return nil
	} else {
		c.mLock.Unlock()
		c.rLock.Lock()
		/*for i, s := range c.rTasks {
			if s != TODO_STATUE {continue}
			c.rTasks[i] = PROC_STATUE
			c.rExpire[i] = time.Now().Unix() + MAX_TASK_TIME
			rsp.TaskSeqNum = i
			rsp.TaskType = RED_TASK
			break
		}*/
		start := c.rtodo
		for {
			
			if c.rTasks[c.rtodo] == TODO_STATUE {
				c.rTasks[c.rtodo] = PROC_STATUE
				c.rExpire[c.rtodo] = time.Now().Unix() + MAX_TASK_TIME
				rsp.TaskSeqNum = c.rtodo
				rsp.TaskType = RED_TASK

				c.rtodo += 1
				if c.rtodo >= c.nReduce {
					c.rtodo -= c.nReduce
				}
				break
			}
			c.rtodo += 1 
			if c.rtodo >= c.nReduce {
				c.rtodo -= c.nReduce
			}
			if c.rtodo == start {
				break
			}
		}
		c.rLock.Unlock()
		return nil
	} 
}

func (c *Coordinator) HandinTask(req *HandinTaskReq, rsp *HandinTaskRsp) error {
	idx := req.TaskSeqNum
	if req.TaskType == MAP_TASK {
		c.mLock.Lock()
		if c.mTasks[idx] == PROC_STATUE {
			c.mTasks[idx] = COMP_STATUE
			c.mCnt++
		}
		c.mLock.Unlock()
	} else if req.TaskType == RED_TASK {
		c.rLock.Lock()
		if c.rTasks[idx] == PROC_STATUE {
			c.rTasks[idx] = COMP_STATUE
			c.rCnt++
		}
		c.rLock.Unlock()
	} else {
		log.Fatal(fmt.Errorf("HandinTask receive invalid taskType(%v)", req.TaskType))
	}
	return nil
}

func (c* Coordinator) RerunTask() {
	for {
		now := time.Now().Unix()
		c.mLock.Lock()
		if c.mCnt < c.nMap {
			for i, s := range c.mTasks {
				if s == PROC_STATUE && now > c.mExpire[i] {
					c.mTasks[i] = TODO_STATUE
					c.mExpire[i] = now + MAX_TASK_TIME
				}
			}
		}
		c.mLock.Unlock()

		now = time.Now().Unix()
		c.rLock.Lock()
		if c.rCnt < c.nReduce {
			for i, s := range c.rTasks {
				if s == PROC_STATUE && now > c.rExpire[i] {
					c.rTasks[i] = TODO_STATUE
					c.rExpire[i] = now + MAX_TASK_TIME
				}
			}
		}
		c.rLock.Unlock()
		time.Sleep(1 * time.Second)
	}
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go c.RerunTask()
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mLock.Lock()
	if c.mCnt != len(c.mTasks) {
		c.mLock.Unlock()
		return false
	}
	c.mLock.Unlock()

	c.rLock.Lock()
	if c.rCnt != len(c.rTasks) {
		c.rLock.Unlock()
		return false
	}
	c.rLock.Unlock()

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:       len(files),
		nReduce:    nReduce,
		inputFiles: files,
		mTasks:     make([]int, len(files)), 
		rTasks:     make([]int, nReduce),    
		mExpire:    make([]int64, len(files)),
		rExpire:    make([]int64, nReduce),
		mtodo: 0,
		rtodo: 0,
	}
	c.server()
	return &c
}
