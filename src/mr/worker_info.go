package mr

import "time"

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

func CreateWorkerInfo(workerID, workerAddress string) *WorkerInfo {
	return &WorkerInfo{
		WorkerID:      workerID,
		Address:       workerAddress,
		Status:        kAlive,
		NumOfMissedHB: 0,
		lastHBTime:    time.Now(),
		FinishedJobs:  []string{},
		OngoingJob:    "Idle",
	}
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
