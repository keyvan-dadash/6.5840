package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestJobArgs struct {
	WorkerID      string
	WorkerAddress string
}

type RequestJobReply struct {
	Job Job
}

type JobDoneArgs struct {
	WorkerID  string
	JobID     string
	JobResult string
}

type JobDoneReply struct {
}

type HeartBeatArgs struct {
	WorkerID      string
	WorkerAddress string
}

type HeartBeatReply struct {
	ReceivedHeartBeat bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
