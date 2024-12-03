package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"6.5840/mr/utils"
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

type WrokerState struct {
	ID         string
	Address    string
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
	logger     *log.Logger
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := &WrokerState{
		ID:         uuid.New().String(),
		Address:    "",
		mapFunc:    mapf,
		reduceFunc: reducef,
		logger:     log.Default(),
	}

	go worker.sendHeartBeatForEver()
	worker.runForEver()
}

func (w *WrokerState) sendHeartBeatForEver() error {
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

func (w *WrokerState) runForEver() error {
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

func (w *WrokerState) runMap(mapJob *MapJob, jobID string) *MapJobResult {
	file, err := os.Open(mapJob.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", mapJob.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapJob.InputFile)
	}
	file.Close()

	result := w.mapFunc(mapJob.InputFile, string(content))
	paritionedKV := w.partitionResult(result, mapJob.PartionNum)

	mapJobResult := &MapJobResult{
		Keys: make(map[string]string),
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
		mapJobResult.Keys[partitionNum] = fileName
	}

	return mapJobResult
}

func (w *WrokerState) partitionResult(result []KeyValue, numOfPartition int) map[string][]KeyValue {
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

func (w *WrokerState) SubmitJobResult(job *Job) error {
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

func (w *WrokerState) runReduce(reduceJob *ReduceJob, reduceJobID int) *ReduceJobResult {
	var intermediate []KeyValue
	for _, inputFile := range reduceJob.InputFiles {
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

	reduceJobResult := &ReduceJobResult{}
	reduceJobResult.OutputFile = fileName
	return reduceJobResult
}

func (w *WrokerState) parseIntermidateFile(fileLocation string) ([]KeyValue, error) {
	file, err := os.Open(fileLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
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
