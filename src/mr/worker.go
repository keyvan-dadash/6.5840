package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"6.5840/mr/utils"
)

var (
	ErrFailedToContactNode = errors.New("failed to contact a node for getting information")
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerState struct {
	ID            string
	Address       string
	mapFunc       func(string, string) []KeyValue
	reduceFunc    func(string, []string) string
	logger        *log.Logger
	producedFiles []string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := &WorkerState{
		ID:         uuid.New().String(),
		Address:    fmt.Sprintf("%v:%v", WorkerIPAddress, WorkerPort),
		mapFunc:    mapf,
		reduceFunc: reducef,
		logger:     log.Default(),
	}

	go worker.sendHeartBeatForEver()
	worker.runForEver()
}

func (w *WorkerState) sendHeartBeatForEver() error {
	timer := time.NewTimer(time.Second * 2)
	for {
		select {
		case <-timer.C:
			{
				args := HeartBeatArgs{
					WorkerID:      w.ID,
					WorkerAddress: w.Address,
				}
				reply := HeartBeatReply{}

				ok := call("Coordinator.HeartBeat", &args, &reply)
				if ok && reply.ReceivedHeartBeat {
					continue
				} else {
					w.logger.Printf("failed to send heartbeat. RPC status: %v, Coordinator response: %v", ok, reply.ReceivedHeartBeat)
				}
			}
		}
	}

}

func (w *WorkerState) runForEver() error {
	failedToContact := 0
	for {
		args := RequestJobArgs{
			WorkerID:      w.ID,
			WorkerAddress: w.Address,
		}

		reply := RequestJobReply{}

		ok := call("Coordinator.RequestJob", &args, &reply)
		if ok {
			failedToContact = 0
			switch reply.Job.JobType {
			case kMapJobType:
				{
					w.logger.Printf(
						"Got new map job. WorkerID: %v, JobID: %v, JobInfo: %v",
						w.ID,
						reply.Job.JobID,
						reply.Job.JobDetail,
					)
					mapJob := &MapJob{}
					utils.FromJson(reply.Job.JobDetail, mapJob)
					mapJobResult := w.runMap(mapJob, reply.Job.JobID)
					if mapJobResult == nil {
						w.logger.Printf(
							"map job failed! worker_id: %v, job_id %v, job_detail: %v\n",
							w.ID,
							reply.Job.JobID,
							reply.Job.JobDetail,
						)
						w.SubmitJobFailed(&reply.Job)
						continue
					}

					reply.Job.SetJobResult(utils.ToJson(mapJobResult))
					w.SubmitJobResult(&reply.Job)
				}
			case kReduceJobType:
				{
					w.logger.Printf(
						"Got new reduce job. WorkerID: %v, JobID: %v, JobInfo: %v",
						w.ID,
						reply.Job.JobID,
						reply.Job.JobDetail,
					)
					reduceJob := &ReduceJob{}
					utils.FromJson(reply.Job.JobDetail, reduceJob)
					reduceJobResult := w.runReduce(reduceJob, reduceJob.ReduceID)
					if reduceJobResult == nil {
						w.logger.Printf(
							"reduce job failed! worker_id: %v, job_id %v, job_detail: %v\n",
							w.ID,
							reply.Job.JobID,
							reply.Job.JobDetail,
						)
						w.SubmitJobFailed(&reply.Job)
						continue
					}

					reply.Job.SetJobResult(utils.ToJson(reduceJobResult))
					w.SubmitJobResult(&reply.Job)
				}
			case kWaitJobType:
				{
					w.logger.Printf(
						"Got new wait job. WorkerID: %v, JobID: %v, JobInfo: %v",
						w.ID,
						reply.Job.JobID,
						reply.Job.JobDetail,
					)
					waitJob := &WaitJob{}
					utils.FromJson(reply.Job.JobDetail, waitJob)
					time.Sleep(time.Duration(waitJob.TimeToWaitSec * int(time.Second)))
				}
			default:
				{
					return ErrJobTypeUnkown
				}
			}
		} else {
			failedToContact++
			if failedToContact == 3 {
				log.Fatal("failed to contact coordinator!")
			}
			time.Sleep(time.Second * 2)
		}
	}
}

func (w *WorkerState) runMap(mapJob *MapJob, jobID string) *MapJobResult {
	file, err := w.fetchFile(&mapJob.InputFile)
	if err != nil {
		if errors.Is(err, ErrFailedToContactNode) {
			// this means coordinator has been failed
			return nil
		} else {
			panic(err)
		}
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapJob.InputFile)
	}
	file.Close()

	result := w.mapFunc(mapJob.InputFile.FileName, string(content))
	paritionedKV := w.partitionResult(result, mapJob.PartionNum)

	mapJobResult := &MapJobResult{
		Keys: make(map[string]File),
	}

	for partitionNum, keyValues := range paritionedKV {
		tempFile, err := os.CreateTemp(
			".",
			fmt.Sprintf("partition-%v-%v-%v-*.txt", w.ID, jobID, partitionNum),
		)
		if err != nil {
			panic(err)
		}
		defer tempFile.Close()

		for _, keyValue := range keyValues {
			fmt.Fprintf(tempFile, "%v %v\n", keyValue.Key, keyValue.Value)
		}

		fileName := fmt.Sprintf("partition-worker-%v-job-%v-%v.txt", w.ID, jobID, partitionNum)
		if err := os.Rename(tempFile.Name(), fileName); err != nil {
			log.Fatal(err)
		}
		mapJobResult.Keys[partitionNum] = CreateFile(
			fileName,
			w.Address,
			"WorkerState.RequestFile",
			IsLocal,
		)
		w.producedFiles = append(w.producedFiles, fileName)
	}

	return mapJobResult
}

func (w *WorkerState) fetchFilesInParallel(inputFiles []File) ([]*os.File, error) {
	osFileCh := make(chan *os.File)
	errCh := make(chan error)
	for _, file := range inputFiles {
		go func(file File) {
			osFile, err := w.fetchFile(&file)
			if err != nil {
				errCh <- err
				return
			}
			osFileCh <- osFile
		}(file)
	}

	osFiles := []*os.File{}
	for i := 0; i < len(inputFiles); i++ {
		select {
		case osFile := <-osFileCh:
			{
				osFiles = append(osFiles, osFile)
			}
		case err := <-errCh:
			{
				return nil, err
			}
		}
	}

	return osFiles, nil
}

func (w WorkerState) fetchFile(inputFile *File) (*os.File, error) {
	if !inputFile.IsLocal && !utils.Contains(w.producedFiles, inputFile.FileName) {
		reply := &RequestFileReply{}
		ok := call(inputFile.RPCName, &RequestFileArgs{
			FileName: inputFile.FileName,
		}, reply)

		if ok {
			if err := utils.Base64ToFile(reply.FileBase64, inputFile.FileName); err != nil {
				panic(err)
			}

		} else {
			return nil, ErrFailedToContactNode
		}
	}

	file, err := os.Open(inputFile.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", inputFile.FileName)
	}
	return file, nil
}

func (w *WorkerState) partitionResult(result []KeyValue, numOfPartition int) map[string][]KeyValue {
	partitionedKV := make(map[string][]KeyValue)
	for _, keyValue := range result {
		paritionNumber := ihash(keyValue.Key) % numOfPartition
		paritionedSet, ok := partitionedKV[strconv.Itoa(paritionNumber)]
		if !ok {
			partitionedKV[strconv.Itoa(paritionNumber)] = []KeyValue{keyValue}
			continue
		}
		paritionedSet = append(paritionedSet, keyValue)
		partitionedKV[strconv.Itoa(paritionNumber)] = paritionedSet
	}

	return partitionedKV
}

func (w *WorkerState) SubmitJobResult(job *Job) error {
	args := JobDoneArgs{
		WorkerID:  w.ID,
		JobID:     job.JobID,
		JobResult: job.jobResult,
	}

	reply := JobDoneReply{}

	ok := call("Coordinator.JobDone", &args, &reply)
	if ok {
		return nil
	} else {
		return errors.New("could not submit the finished job")
	}
}

func (w *WorkerState) SubmitJobFailed(job *Job) error {
	args := JobFailedArgs{
		WorkerID: w.ID,
		JobID:    job.JobID,
	}

	reply := JobFailedReply{}

	ok := call("Coordinator.JobFailed", &args, &reply)
	if ok {
		return nil
	} else {
		return errors.New("could not submit the failed job")
	}
}

func (w *WorkerState) runReduce(reduceJob *ReduceJob, reduceJobID int) *ReduceJobResult {
	intermidateFiles, err := w.fetchFilesInParallel(reduceJob.InputFiles)
	if err != nil {
		if errors.Is(err, ErrFailedToContactNode) {
			return nil
		}
		panic(err)
	}

	var intermediate []KeyValue
	for _, inputFile := range intermidateFiles {
		tmpKeyValue, err := w.parseIntermidateFile(inputFile)
		if err != nil {
			panic(err)
		}
		intermediate = append(intermediate, tmpKeyValue...)
	}

	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%v-*.txt", w.ID))
	if err != nil {
		panic(err)
	}
	defer tempFile.Close()

	sort.Sort(ByKey(intermediate))

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
		output := w.reduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	fileName := fmt.Sprintf("mr-out-%v.txt", reduceJobID)
	if err := os.Rename(tempFile.Name(), fileName); err != nil {
		log.Fatal(err)
	}
	w.producedFiles = append(w.producedFiles, fileName)

	fileBase64, err := utils.FileToBase64(fileName)
	if err != nil {
		panic(err)
	}

	return CreateReduceJobResult(fileName, fileBase64)
}

func (w *WorkerState) parseIntermidateFile(file *os.File) ([]KeyValue, error) {
	var keyValues []KeyValue
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue // Skip empty lines
		}

		var kv KeyValue
		_, err := fmt.Sscanf(line, "%s %s", &kv.Key, &kv.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse line '%s': %v", line, err)
		}

		keyValues = append(keyValues, kv)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	return keyValues, nil
}

func (w *WorkerState) RequestFile(args *RequestFileArgs, reply *RequestFileReply) error {
	if !utils.Contains(w.producedFiles, args.FileName) {
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

// start a thread that listens for RPCs from worker.go
func (w *WorkerState) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%v", WorkerPort))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	var c *rpc.Client
	var err error
	if !IsLocal {
		c, err = rpc.DialHTTP("tcp", CoordinatorIPAddress+":"+strconv.Itoa(CoordinatorPort))
	} else {
		sockname := coordinatorSock()
		c, err = rpc.DialHTTP("unix", sockname)
	}

	if err != nil {
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
