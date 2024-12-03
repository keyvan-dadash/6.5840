package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"6.5840/mr/utils"
)

// Coordinator holds information about a Coordinator
type Coordinator struct {
	// Workers that are available
	Workers map[string]*WorkerInfo

	// InputFiles to be processed
	InputFiles []string

	// Jobs are the total jobs that need to be done
	Jobs map[string]*Job

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
		Jobs:           make(map[string]*Job),
	}

	for _, file := range files {
		mapJobInfo := CreateMapJob(file, nReduce)
		job := CreateJob(kMapJobType)
		job.SetJobDetail(utils.ToJson(mapJobInfo))
		c.Jobs[job.JobID] = job
	}

	for i := 0; i < nReduce; i++ {
		reduceJobInfo := CreaReduceJob(i, []string{})
		job := CreateJob(kReduceJobType)
		job.SetJobDetail(utils.ToJson(reduceJobInfo))
		c.Jobs[job.JobID] = job
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
		worker = CreateWorkerInfo(args.WorkerID, args.WorkerAddress)
		c.Workers[args.WorkerID] = worker
	}
	worker.ReceivedHB()

	c.mapJobsDone = false
	c.reduceJobsDone = false

	mapJob, shouldWait := c.getMapJob()
	if mapJob != nil {
		mapJob.PutJobOnGoing()
		reply.Job = *mapJob
		worker.SetOngoingJob(mapJob.JobID)
		return nil
	}

	if shouldWait {
		waitJob := CreateWaitJob(2)
		job := CreateJob(kWaitJobType)
		job.SetJobDetail(utils.ToJson(waitJob))
		reply.Job = *job
		worker.PutWorkerOnWait()
		return nil
	}

	c.mapJobsDone = true

	reduceJob, shouldWait := c.getReduceJob()
	if reduceJob != nil {
		reduceJob.PutJobOnGoing()
		reply.Job = *reduceJob
		worker.SetOngoingJob(reduceJob.JobID)
		return nil
	}

	if shouldWait {
		waitJob := CreateWaitJob(2)
		job := CreateJob(kWaitJobType)
		job.SetJobDetail(utils.ToJson(waitJob))
		reply.Job = *job
		worker.PutWorkerOnWait()
		return nil
	}

	c.reduceJobsDone = true

	// TODO: instruct worker to exit
	return nil
}

func (c *Coordinator) getMapJob() (*Job, bool) {
	for _, job := range c.Jobs {
		if !job.IsMapJob() {
			continue
		}

		if job.jobStatus == kUnfinishedJob || job.jobStatus == kFailedJob {
			return job, false
		} else if job.jobStatus == kOngoingJob {
			return nil, true
		}
	}

	return nil, false
}

func (c *Coordinator) getReduceJob() (*Job, bool) {
	ongoingReduceJob := false
	intermediateFiles := []string{}
	for _, reduceJobInfo := range c.Jobs {
		if !reduceJobInfo.IsReduceJob() {
			continue
		}

		if reduceJobInfo.jobStatus == kFinishedJob || reduceJobInfo.jobStatus == kOngoingJob {
			if reduceJobInfo.jobStatus == kOngoingJob {
				ongoingReduceJob = true
			}
			continue
		}

		for _, mapJobInfo := range c.Jobs {
			if !mapJobInfo.IsMapJob() {
				continue
			}

			reduceJobDetail := &ReduceJob{}
			utils.FromJson(reduceJobInfo.JobDetail, reduceJobDetail)

			mapJobResult := &MapJobResult{}
			utils.FromJson(mapJobInfo.jobResult, mapJobResult)
			fileLocation, ok := mapJobResult.Keys[strconv.Itoa(reduceJobDetail.ReduceID)]
			if ok {
				intermediateFiles = append(intermediateFiles, fileLocation)
			}
		}

		reduceJob := &ReduceJob{}
		utils.FromJson(reduceJobInfo.JobDetail, reduceJob)
		reduceJob.InputFiles = intermediateFiles
		reduceJobInfo.SetJobDetail(utils.ToJson(reduceJob))

		return reduceJobInfo, false
	}

	return nil, ongoingReduceJob
}

func (c *Coordinator) JobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	worker, _ := c.Workers[args.WorkerID]
	worker.ReceivedHB()
	job, _ := c.Jobs[args.JobID]
	if worker.IsOngoingJob() {
		job.SetJobResult(args.JobResult)
		job.jobStatus = kFinishedJob
		worker.FinishedCurrentJob()
		worker.ReceivedHB()
	}
	return nil
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	worker, isExistingWorker := c.Workers[args.WorkerID]
	if !isExistingWorker {
		worker = CreateWorkerInfo(args.WorkerID, args.WorkerAddress)
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
		job.FailedJob()
	}

	for _, jobID := range worker.FinishedJobs {
		job, _ := c.Jobs[jobID]
		if job.IsMapJob() {

			// In the local mode for passing the crash test the below line should be commented.
			// The reason is that if a worker fails, we have to do its crossponding map tasks again
			// so that subsequent reduce tasks start, otherwise we keep re-assigning map task since
			// the coordinator assumes that the worker failed so other workers dont have access to the failed worker's map tasks' files.
			// Thus, the coordinator reschedules map tasks agian. However, as TA stated in discussion, a done task should not be redone.
			// Therefore, for keeping TA's word we have to comment this. Nonetheless, in case of disitrbuted implementation, as it is stated in the bonus part,
			// we have to have this line so that we have correct implementation.
			//job.FailedJob()
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
