package mr

import (
	"context"
	"fmt"
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

	CoordinatorAddress string

	// InputFiles to be processed
	InputFiles []string

	// Jobs are the total jobs that need to be done
	Jobs map[string]*Job

	// NReduce of reduce jobs
	NReduce int

	mapJobsDone bool

	reduceJobsDone bool

	requestLock *sync.Mutex

	endLogFunc context.CancelFunc

	logCtx context.Context
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce jobs to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	logCtx, cancel := context.WithCancel(context.Background())
	c := Coordinator{
		Workers:            make(map[string]*WorkerInfo),
		CoordinatorAddress: fmt.Sprintf("%v:%v", CoordinatorIPAddress, CoordinatorPort),
		InputFiles:         files,
		NReduce:            nReduce,
		mapJobsDone:        false,
		reduceJobsDone:     false,
		requestLock:        &sync.Mutex{},
		Jobs:               make(map[string]*Job),
		endLogFunc:         cancel,
		logCtx:             logCtx,
	}

	for _, file := range files {
		mapJobInfo := CreateMapJob(
			CreateFile(file, c.CoordinatorAddress, "Coordinator.RequestFile", IsLocal),
			nReduce,
		)
		job := CreateJob(kMapJobType)
		job.SetJobDetail(utils.ToJson(mapJobInfo))
		c.Jobs[job.JobID] = job
	}

	for i := 0; i < nReduce; i++ {
		reduceJobInfo := CreaReduceJob(i, []File{})
		job := CreateJob(kReduceJobType)
		job.SetJobDetail(utils.ToJson(reduceJobInfo))
		c.Jobs[job.JobID] = job
	}

	c.server()
	go c.observeWorkers()
	go func() {
		err := c.writeSimpleLogs(c.logCtx, "coordinator_log.txt")
		if err != nil {
			panic(err)
		}
	}()

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
	intermediateFiles := []File{}
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

func (c *Coordinator) JobFailed(args *JobFailedArgs, reply *JobFailedReply) error {
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	worker, _ := c.Workers[args.WorkerID]
	worker.ReceivedHB()
	job, _ := c.Jobs[args.JobID]
	if worker.IsOngoingJob() {
		job.jobStatus = kFailedJob
		worker.FailCurrentJob()
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

func (c *Coordinator) RequestFile(args *RequestFileArgs, reply *RequestFileReply) error {
	if !utils.Contains(c.InputFiles, args.FileName) {
		// TODO: eroor?
		panic(nil)
	}

	fileBase64, err := utils.FileToBase64(args.FileName)
	if err != nil {
		return err
	}
	reply.FileBase64 = fileBase64
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
		if job.IsMapJob() && !IsLocal {
			job.FailedJob()
			continue
		}
	}

	worker.ClearFinishedJobs()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	var l net.Listener
	var e error

	if !IsLocal {
		l, e = net.Listen("tcp", fmt.Sprintf(":%v", CoordinatorPort))
	} else {
		sockname := coordinatorSock()
		os.Remove(sockname)
		l, e = net.Listen("unix", sockname)
	}

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) writeSimpleLogs(ctx context.Context, filePath string) error {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			finishedLog := fmt.Sprintf("finished - %s\n", time.Now().Format(time.RFC3339))
			if _, err := file.WriteString(finishedLog); err != nil {
				return fmt.Errorf("failed to write to log file: %v", err)
			}
			return nil
		case <-ticker.C:
			aliveLog := fmt.Sprintf("I am alive - %s\n", time.Now().Format(time.RFC3339))
			if _, err := file.WriteString(aliveLog); err != nil {
				return fmt.Errorf("failed to write to log file: %v", err)
			}
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.requestLock.Lock()
	defer c.requestLock.Unlock()

	ret := c.mapJobsDone && c.reduceJobsDone

	if ret && !IsLocal {
		// dump reduce files to local
		for _, job := range c.Jobs {
			if !job.IsReduceJob() {
				continue
			}

			reduceJobDetail := &ReduceJob{}
			utils.FromJson(job.JobDetail, reduceJobDetail)

			reduceJobResult := &ReduceJobResult{}
			utils.FromJson(job.jobResult, reduceJobResult)

			fileName := fmt.Sprintf("mr-out-%v.txt", reduceJobDetail.ReduceID)
			if err := utils.Base64ToFile(reduceJobResult.OutputFileBase64, fileName); err != nil {
				panic(err)
			}
		}
	}

	if ret {
		c.endLogFunc()
		time.Sleep(1 * time.Second)
	}

	return ret
}
