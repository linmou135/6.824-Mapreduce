package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	File           []string
	Taskid         int
	Maptask        chan *Task
	Reducetask     chan *Task
	Taskinfomation map[int]*Taskinfo
	Reducenums     int
	Phrase         string
}
type Taskinfo struct {
	During_Time time.Time
	State       string
	TaskArr     *Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := false
	if c.Phrase == "Exittask" {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		File:           files,
		Maptask:        make(chan *Task, len(files)),
		Reducenums:     nReduce,
		Reducetask:     make(chan *Task, nReduce),
		Taskinfomation: make(map[int]*Taskinfo, nReduce+len(files)),
		Phrase:         "Maptask",
	}
	fmt.Println(c)
	// Your code here.
	//创造Mapwork
	c.MakeMapwork(files)
	c.server()
	//开启监听任务
	go c.MakeWatch()
	return &c
}
func (c *Coordinator) Increaseid() int { //用来id自增，防止出现不同id
	id := c.Taskid
	c.Taskid++
	return id
}
func (c *Coordinator) MakeMapwork(files []string) {
	//将每一个file文件都转化为一个mapwork
	for _, file := range files {
		id := c.Increaseid()
		task := Task{
			Taskid:     id,
			Taskfile:   []string{file},
			Reducenums: c.Reducenums,
			Type:       "Map",
			//duringtime和state等到时候分派任务的时候再分配
		}
		//	fmt.Println(task)
		c.Maptask <- &task
		c.Taskinfomation[id] = &Taskinfo{
			State:   "Waitting",
			TaskArr: &task,
		}
		//	fmt.Println(c.Taskinfomation[id])
	}
}
func selctfiles(idx int) []string {
	var s []string
	path, _ := os.Getwd()
	file, _ := os.ReadDir(path)
	for _, files := range file {
		if strings.HasSuffix(files.Name(), "-"+strconv.Itoa(idx)) && strings.HasPrefix(files.Name(), "mr-temp") {
			s = append(s, files.Name())
		}
	}
	return s
}
func (c *Coordinator) MakeReducework() {
	//将每一个file文件都转化为一个mapwork
	for i := 0; i < c.Reducenums; i++ {
		id := c.Increaseid()
		task := Task{
			Taskid:     id,
			Taskfile:   selctfiles(i),
			Reducenums: c.Reducenums,
			Type:       "Reduce",
			//duringtime和state等到时候分派任务的时候再分配
		}
		//	fmt.Println(task)
		c.Reducetask <- &task
		c.Taskinfomation[id] = &Taskinfo{
			State:   "Waitting",
			TaskArr: &task,
		}
	}
}
func (c *Coordinator) MakeWatch() { //监听是否存在超时任务
	for {
		mu.Lock()
		if c.Phrase == "Exit" {
			return
		}
		time.Sleep(3 * time.Second) //休息一会儿免得别的任务还没启动就已经被判定了
		for _, meth := range c.Taskinfomation {
			if meth.State == "Working" && time.Since(meth.During_Time) > 9*time.Second {
				meth.State = "Waitting"
				if meth.TaskArr.Type == "Map" {
					task := meth.TaskArr
					c.Maptask <- task
				} else if meth.TaskArr.Type == "Reduce" {
					task := meth.TaskArr
					c.Reducetask <- task
				}
			}
		}
		mu.Unlock()
	}
}
func (c *Coordinator) Tonext() {
	//到下一阶段
	code := c.Panding()
	fmt.Printf("%v %v\n", c.Phrase, code)
	if c.Phrase == "Maptask" && code == 1 {
		//	fmt.Println(222222)
		c.Phrase = "Reducetask"
		c.MakeReducework()
	}
	if c.Phrase == "Reducetask" && code == 2 {
		c.Phrase = "Exittask"
	}
}
func (c *Coordinator) Panding() int { //判定是否所有的当前任务都已经完成，是的话进入下一个阶段
	cnt := 0
	cnt1 := 0
	for _, mp := range c.Taskinfomation {
		//	fmt.Println(mp)
		if mp.State == "Done" && mp.TaskArr.Type == "Map" {
			cnt++
		}
		if mp.State == "Done" && mp.TaskArr.Type == "Reduce" {
			cnt1++
		}
	}
	fmt.Printf("%v %v %v\n", cnt, cnt1, len(c.File))
	if cnt1 == c.Reducenums {
		return 2
	}
	if cnt == len(c.File) {
		return 1
	}
	return 0
}
func (c *Coordinator) AssignTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	reply.Type = "Wait"
	switch c.Phrase {
	case "Maptask":
		{
			if len(c.Maptask) > 0 {
				*reply = *<-c.Maptask //把任务给他
				id := reply.Taskid
				c.Taskinfomation[id].During_Time = time.Now()
				c.Taskinfomation[id].State = "Working"
			} else {
				reply.Type = "Wait"
				c.Tonext()
			}
		}
	case "Reducetask":
		{
			if len(c.Reducetask) > 0 {
				*reply = *<-c.Reducetask //把任务给他
				id := reply.Taskid
				c.Taskinfomation[id].During_Time = time.Now()
				c.Taskinfomation[id].State = "Working"
			} else {
				reply.Type = "Wait"
				c.Tonext()
			}
		}
	case "Exittask":
		{
			reply.Type = "Exit" //请求已经完全结束了，别请求了，没任务给你做了
			fmt.Printf("Task is Alldone\n")
			return nil
		}
	default:
		{
			fmt.Printf("出现之前没出现的任务了\n")
		}
	}
	return nil
}
func (c *Coordinator) Finished(args *Task, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	id := args.Taskid
	c.Taskinfomation[id].State = "Done"
	return nil
}
