package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	flag := true
	for flag {
		task := GetTask()
		switch task.Type {
		case "Map":
			{
				Mapwork(mapf, &task)
				calldone(&task)
			}
		case "Reduce":
			{
				Reducework(reducef, &task)
				calldone(&task)
			}
		case "Exit":
			{
				fmt.Printf("taskalldon\n")
				return
			}
		case "Wait":
			{
				fmt.Println("已经进来休息过了")
				time.Sleep(5 * time.Second)
			}
		}
		time.Sleep(2 * time.Second)
		//	fmt.Println(task)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}
func calldone(f *Task) {
	args := f
	reply := Task{}
	ok := call("Coordinator.Finished", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//	fmt.Printf("reply:=%v\n", reply)
	} else {
		//	fmt.Printf("call failed!\n")

	}
}
func Mapwork(mapf func(string, string) []KeyValue, task *Task) {
	file := task.Taskfile[0]
	Text, _ := os.Open(file)
	defer Text.Close()
	str, _ := ioutil.ReadAll(Text)
	nreduce := task.Reducenums
	HashKV := make([][]KeyValue, nreduce)
	content := mapf(file, string(str))
	for _, s := range content {
		key := ihash(string(s.Key)) % nreduce
		HashKV[key] = append(HashKV[key], s)
	}
	for i := 0; i < nreduce; i++ {
		fn := "mr-temp-" + strconv.Itoa(task.Taskid) + "-" + strconv.Itoa(i)
		fi, _ := os.Create(fn)
		defer fi.Close()
		enc := json.NewEncoder(fi)
		for _, kv := range HashKV[i] {
			_ = enc.Encode(&kv)
		}
	}
}
func shuffle(files []string) []KeyValue {
	var KV []KeyValue
	for _, file := range files {
		fi, _ := os.Open(file)
		defer fi.Close()
		dec := json.NewDecoder(fi)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			KV = append(KV, kv)
		}
	}
	sort.Sort(ByKey(KV))
	return KV
}
func Reducework(reducef func(string, []string) string, task *Task) {
	path, _ := os.Getwd()
	ofile, _ := ioutil.TempFile(path, "mr-temp-*")
	intermediate := shuffle(task.Taskfile)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	filename := "mr-out-" + strconv.Itoa(task.Taskid)
	os.Rename(ofile.Name(), filename)
}
func GetTask() Task {
	reply := Task{}
	args := TaskArgs{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	//fmt.Println(reply)
	if ok {
		// reply.Y should be 100.
		//	fmt.Printf("reply:=%v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
