package mr

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	kAlive = iota

	kDead
)

// WorkerInfo holds information of a worker
type WorkerInfo struct {
	// WorkerID shows the id of the worker
	WorkerID string

	// Address of the worker in case of contacting
	Address string

	// Status of the worker whether is alive or dead
	Status int8

	// NumOfMissedHB shows how many heatbeats has been passed since this worker did not respond
	NumOfMissedHB int

	lastHBTime time.Time

	// FinishedJobs holds job's id of jobs that this worker has finished
	FinishedJobs []string

	// OngoingJob holds ID of a job that this worker is doing
	OngoingJob string
}

func (w *WorkerInfo) FinishedCurrentJob() {
	w.FinishedJobs = append(w.FinishedJobs, w.OngoingJob)
	w.OngoingJob = "Idle"
}

func (w *WorkerInfo) FailCurrentJob() string {
	currentJob := w.OngoingJob
	w.OngoingJob = "Idle"
	return currentJob
}

func (w *WorkerInfo) ClearFinishedJobs() {
	w.FinishedJobs = []string{}
}

func (w *WorkerInfo) SetOngoingJob(jobID string) {
	w.OngoingJob = jobID
}

func (w *WorkerInfo) PutWorkerOnWait() {
	w.OngoingJob = "wait"
}

func (w WorkerInfo) IsOngoingJob() bool {
	if w.OngoingJob == "Idle" || w.OngoingJob == "wait" {
		return false
	}
	return true
}

func (w *WorkerInfo) ReceivedHB() {
	w.NumOfMissedHB = 0
	w.Status = kAlive
	w.lastHBTime = time.Now()
}

func (w *WorkerInfo) CheckHBWithTime(baseDuration time.Duration) {
	passedDuration := time.Now().Sub(w.lastHBTime)
	w.NumOfMissedHB += int(passedDuration / baseDuration)
	if w.NumOfMissedHB > 5 {
		w.Status = kDead
	}
}

func (w *WorkerInfo) MarkAlive() {
	w.Status = kAlive
}

func (w *WorkerInfo) MarkDead() {
	w.Status = kDead
}

func (w *WorkerInfo) IsDead() bool {
	return w.Status == kDead
}

type JobInfo struct {
	Job       Job
	JobResult JobResult
	JobStatus int8
}

func (j *JobInfo) IsMapJob() bool {
	return j.Job.JobType == kMapJobType
}

func (j *JobInfo) IsReduceJob() bool {
	return j.Job.JobType == kReduceJobType
}

func (j *JobInfo) FailJob() {
	j.JobStatus = kFailedJob
	j.JobResult = JobResult{}
}

// Coordinator holds information about a Coordinator
type Coordinator struct {
	// Workers that are available
	Workers map[string]*WorkerInfo

	// InputFiles to be processed
	InputFiles []string

	// Jobs are the total jobs that need to be done
	Jobs map[string]*JobInfo

	// NReduce of reduce jobs
	NReduce int

	mapJobsDone bool

	reduceJobsDone bool

	requestLock *sync.Mutex
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce jobs to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Workers:        make(map[string]*WorkerInfo),
		InputFiles:     files,
		NReduce:        nReduce,
		mapJobsDone:    false,
		reduceJobsDone: false,
		requestLock:    &sync.Mutex{},
		Jobs:           make(map[string]*JobInfo),
	}

	for _, file := range files {
		mapJobInfo := &MapJob{InputFile: file, PartionNum: nReduce}
		b, err := json.Marshal(mapJobInfo)
		if err != nil {
			panic(err)
		}

		jobID := uuid.New().String()
		c.Jobs[jobID] = &JobInfo{
			Job: Job{
				JobID:   jobID,
				JobType: kMapJobType,
				JobInfo: string(b),
			},
			JobStatus: kUnfinishedJob,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.Jobs[strconv.Itoa(i)] = &JobInfo{
			Job: Job{
				JobID:   strconv.Itoa(i),
				JobType: kReduceJobType,
			},
			JobStatus: kUnfinishedJob,
		}
	}

	c.server()
	go c.observeWorkers()

	return &c
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	worker, isExistingWorker := c.Workers[args.WorkerID]
	if !isExistingWorker {
		worker = &WorkerInfo{
			WorkerID:      args.WorkerID,
			Address:       args.WorkerAddress,
			Status:        kAlive,
			NumOfMissedHB: 0,
		}
		c.Workers[args.WorkerID] = worker
	}
	worker.ReceivedHB()

	onGoingJob := false
	for _, jobInfo := range c.Jobs {
		if !jobInfo.IsMapJob() {
			continue
		}

		if jobInfo.JobStatus == kUnfinishedJob || jobInfo.JobStatus == kFailedJob {
			reply.Job = jobInfo.Job
			jobInfo.JobStatus = kOngoingJob
			worker.SetOngoingJob(jobInfo.Job.JobID)
			return nil
		} else if jobInfo.JobStatus == kOngoingJob {
			onGoingJob = true
		}
	}

	if onGoingJob {
		waitJob := &WaitJob{TimeToWaitSec: 2}
		b, err := json.Marshal(waitJob)
		if err != nil {
			panic(err)
		}

		reply.Job = Job{
			JobID:   "",
			JobType: kWaitJobType,
			JobInfo: string(b),
		}

		worker.PutWorkerOnWait()
		return nil
	}

	c.mapJobsDone = true

	// Reduce phase
	ongoingReduceJob := false
	intermediateFiles := []string{}
	for _, reduceJobInfo := range c.Jobs {
		if !reduceJobInfo.IsReduceJob() {
			continue
		}

		if reduceJobInfo.JobStatus == kFinishedJob || reduceJobInfo.JobStatus == kOngoingJob {
			if reduceJobInfo.JobStatus == kOngoingJob {
				ongoingReduceJob = true
			}

			continue
		}

		for _, mapJobInfo := range c.Jobs {
			if !mapJobInfo.IsMapJob() {
				continue
			}

			mapJobResult := &MapJobResult{}
			if err := json.Unmarshal([]byte(mapJobInfo.JobResult.JobResultInfo), mapJobResult); err != nil {
				panic(err)
			}
			fileLocation, ok := mapJobResult.Keys[reduceJobInfo.Job.JobID]
			if ok {
				intermediateFiles = append(intermediateFiles, fileLocation)
			}
		}

		reduceJob := &ReduceJob{
			InputFiles: intermediateFiles,
		}
		b, err := json.Marshal(reduceJob)
		if err != nil {
			panic(err)
		}

		reduceJobInfo.Job.JobInfo = string(b)
		reduceJobInfo.JobStatus = kOngoingJob

		reply.Job = reduceJobInfo.Job
		worker.SetOngoingJob(reduceJobInfo.Job.JobID)
		return nil
	}

	if !ongoingReduceJob {
		c.reduceJobsDone = true
	}
	waitJob := &WaitJob{TimeToWaitSec: 2}
	b, err := json.Marshal(waitJob)
	if err != nil {
		panic(err)
	}

	reply.Job = Job{
		JobID:   "",
		JobType: kWaitJobType,
		JobInfo: string(b),
	}
	worker.PutWorkerOnWait()
	return nil
}

func (c *Coordinator) JobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	worker, _ := c.Workers[args.WorkerID]
	worker.ReceivedHB()
	switch args.JobResult.JobType {
	case kMapJobType:
		{
			jobInfo := c.Jobs[args.JobResult.JobID]
			if worker.IsOngoingJob() {
				jobInfo.JobResult = args.JobResult
				jobInfo.JobStatus = kFinishedJob
				worker.FinishedCurrentJob()
				worker.ReceivedHB()
			}
			return nil
		}
	case kReduceJobType:
		{
			jobInfo := c.Jobs[args.JobResult.JobID]
			if worker.IsOngoingJob() {
				jobInfo.JobResult = args.JobResult
				jobInfo.JobStatus = kFinishedJob
				worker.FinishedCurrentJob()
				worker.ReceivedHB()
			}
			return nil
		}
	}
	return ErrJobTypeUnkown
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	worker, isExistingWorker := c.Workers[args.WorkerID]
	if !isExistingWorker {
		worker = &WorkerInfo{
			WorkerID:      args.WorkerID,
			Address:       args.WorkerAddress,
			Status:        kAlive,
			NumOfMissedHB: 0,
		}
		c.Workers[args.WorkerID] = worker
	}
	worker.ReceivedHB()
	reply.ReceivedHeartBeat = true
	return nil
}

func (c *Coordinator) observeWorkers() error {
	timer := time.NewTimer(time.Second * 2)
	for {
		select {
		case <-timer.C:
			{
				c.requestLock.Lock()
				for _, worker := range c.Workers {
					worker.CheckHBWithTime(time.Second * 2)
					if worker.IsDead() {
						c.handleFailedWorker(worker)
					}
				}
				c.requestLock.Unlock()
				timer.Reset(time.Second * 2)
			}
		}
	}
}

func (c *Coordinator) handleFailedWorker(worker *WorkerInfo) {
	if worker.IsOngoingJob() {
		ongoingJobID := worker.FailCurrentJob()
		job, _ := c.Jobs[ongoingJobID]
		job.FailJob()
	}

	for _, jobID := range worker.FinishedJobs {
		job, _ := c.Jobs[jobID]
		if job.IsMapJob() {
			job.FailJob()
			continue
		}
	}

	worker.ClearFinishedJobs()
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
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	ret := c.mapJobsDone && c.reduceJobsDone
	return ret
}
