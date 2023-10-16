package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
type Task struct {
	Taskid     int      //id
	Taskfile   []string //任务过程中要用到的文件
	Reducenums int      //要开几个reduce
	Type       string   //当前是什么任务,map还是reduce还是全部做完
}
type TaskArgs struct { //定义一个无用的

}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
