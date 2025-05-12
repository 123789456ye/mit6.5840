package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var (
		rsp *GetTaskRsp
		err error
	)
	for {
		rsp, err = getTaskBlock()
		if err != nil {
			fmt.Printf("Worker getTask Error")
			return
		}
		if rsp.TaskType == MAP_TASK {
			file, err := os.Open(rsp.InputFileName)
			if err != nil {
				log.Fatalf("MAPTASK cannot open %v", rsp.InputFileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("Cannot read %v", rsp.InputFileName)
			}
			file.Close()
			kva := mapf(rsp.InputFileName, string(content))
			buckets := make([][]KeyValue, rsp.NReduce)
			for _, kv := range kva {
				idx := ihash(kv.Key) % rsp.NReduce
				buckets[idx] = append(buckets[idx], kv)
			}
			for i := 0; i < rsp.NReduce; i++ {
				fp, err := os.CreateTemp("", "maptemp")
				if err != nil {
					log.Fatal(err)
				}
				enc := json.NewEncoder(fp)
				if err = enc.Encode(buckets[i]); err != nil {
					log.Fatal(err)
				}
				if err = os.Rename(fp.Name(), fmt.Sprintf(MED_FORMAT, rsp.TaskSeqNum, i)); err != nil {
					log.Fatal(err)
				}
				fp.Close()
			}
		} else if rsp.TaskType == RED_TASK{
			input := []KeyValue{}
			for i := 0; i < rsp.NMap; i++ {
				file, err := os.Open(fmt.Sprintf(MED_FORMAT, i, rsp.TaskSeqNum))
				if err != nil {
					log.Fatalf("Cannot read intermediate file")
				}
				dec := json.NewDecoder(file)
				bucket := []KeyValue{}
				if err = dec.Decode(&bucket); err != nil {
					log.Fatal(err)
				}
				input = append(input, bucket...)
				file.Close()
			}
			sort.Slice(input, func(i int, j int) bool {return input[i].Key < input[j].Key})
			fp, err := os.CreateTemp("", "outputfile")
			if err != nil{
				log.Fatal(err)
			}
			i := 0
			for i < len(input) {
				j := i + 1
				for j < len(input) && input[j].Key == input[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, input[k].Value)
				}
				output := reducef(input[i].Key, values)

				fmt.Fprintf(fp, "%v %v\n", input[i].Key, output)

				i = j
			}
			if err = os.Rename(fp.Name(), fmt.Sprintf(OUT_FORMAT, rsp.TaskSeqNum)); err != nil {
				log.Fatal(err)
			}
			fp.Close()
		} else {
			fmt.Printf("Worker getTask unknown task type(%d)", rsp.TaskType)
			return
		}
		if err := handinTask(rsp.TaskType, rsp.TaskSeqNum); err != nil {
			fmt.Printf("Worker handinTask fail: %v", err)
			return
		}
	}

}

func getTaskBlock() (*GetTaskRsp, error) {
	for {
		rsp, err := getTask()
		if err != nil {
			os.Exit(0)
		}
		if rsp != nil && rsp.TaskSeqNum >= 0 {
			return rsp, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func getTask() (*GetTaskRsp, error) {
	req := GetTaskReq{}
	rsp := GetTaskRsp{}
	if ok := call("Coordinator.GetTask", &req, &rsp); ok {
		return &rsp, nil
	} else {
		return nil, fmt.Errorf("GetTask Error")
	}
}

func handinTask(tasktype int, taskSeqNum int) error {
	req := HandinTaskReq{tasktype, taskSeqNum}
	rsp := HandinTaskRsp{}
	if ok := call("Coordinator.HandinTask", &req, &rsp); ok {
		return nil
	} else {
		return fmt.Errorf("HandinTask Error")
	}
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
