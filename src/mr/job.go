package mr

import (
	"errors"

	"github.com/google/uuid"
)

const (
	kUnkownJobType = iota

	kReduceJobType

	kMapJobType

	kWaitJobType
)

const (
	kUnfinishedJob = iota

	kOngoingJob

	kFailedJob

	kFinishedJob
)

var (
	ErrJobTypeUnkown = errors.New("given job type is unkown")
)

// Job is a struct that holds information about a job that has to be done
type Job struct {

	// JobID holds ID of a job which is unique
	JobID string

	// JobType shows type of the job which is map or reduce
	JobType int8

	// JobDetail is a json field that holds information of a job which would be MapJob or ReduceJob
	JobDetail string

	jobStatus int8

	jobResult string
}

func CreateJob(jobType int8) *Job {
	jobID := uuid.New().String()
	return &Job{
		JobID:     jobID,
		JobType:   jobType,
		JobDetail: "",
		jobStatus: kUnfinishedJob,
		jobResult: "",
	}
}

func (j *Job) SetJobType(jobType int8) {
	j.JobType = jobType
}

func (j *Job) SetJobDetail(jobDetail string) {
	j.JobDetail = jobDetail
}

func (j *Job) IsMapJob() bool {
	return j.JobType == kMapJobType
}

func (j *Job) IsReduceJob() bool {
	return j.JobType == kReduceJobType
}

func (j *Job) PutJobOnGoing() {
	j.jobStatus = kOngoingJob
}

func (j *Job) FailedJob() {
	j.jobStatus = kFailedJob
	j.jobResult = ""
}

func (j *Job) FinishedJob() {
	j.jobStatus = kFinishedJob
}

func (j *Job) SetJobResult(jobResult string) {
	j.jobResult = jobResult
}

type MapJob struct {
	InputFile  string
	PartionNum int
}

func CreateMapJob(InputFile string, ParitionNum int) *MapJob {
	return &MapJob{
		InputFile:  InputFile,
		PartionNum: ParitionNum,
	}
}

type ReduceJob struct {
	ReduceID   int
	InputFiles []string
}

func CreaReduceJob(reduceID int, inputFiles []string) *ReduceJob {
	return &ReduceJob{
		ReduceID:   reduceID,
		InputFiles: inputFiles,
	}
}

type WaitJob struct {
	TimeToWaitSec int
}

func CreateWaitJob(timeToWait int) *WaitJob {
	return &WaitJob{
		TimeToWaitSec: timeToWait,
	}
}

type MapJobResult struct {
	Keys map[string]string
}

func CreaMapJobResult() *MapJobResult {
	return &MapJobResult{
		Keys: make(map[string]string),
	}
}

type ReduceJobResult struct {
	OutputFile string
}

func CreateReduceJobResult(outputFile string) *ReduceJobResult {
	return &ReduceJobResult{
		OutputFile: outputFile,
	}
}
