package mr

import "errors"

const (
	kReduceJobType = iota

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

	// JobInfo is a json field that holds information of a job which would be MapJob or ReduceJob
	JobInfo string
}

type MapJob struct {
	InputFile  string
	PartionNum int
}

type ReduceJob struct {
	InputFiles []string
}

type WaitJob struct {
	TimeToWaitSec int
}

type JobResult struct {
	// JobID holds ID of a job which is unique
	JobID string

	// JobType shows type of the job which is map or reduce
	JobType int8

	// JobResultInfo is a json field that holds information of a job which would be MapJob or ReduceJob
	JobResultInfo string
}

type MapJobResult struct {
	Keys map[string]string
}

type ReduceJobResult struct {
	OutputFile string
}
