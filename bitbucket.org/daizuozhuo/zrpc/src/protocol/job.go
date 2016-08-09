package protocol

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

type Job interface {
	GetParent() string
	GetID() string
	SetID(string)
	GetType() string
	GetUser() string
	GetArgs() []string
	Update(Status, float64)
	GetStatus() Status
	Save()
}

type BasicJob struct {
	ID      string
	Type    string
	User    string
	Parent  string //job id of parent process
	Args    []string
	Cache   []int
	Threads int //-1 means use all threads in the machine
	GPU     bool

	Status
}

func (b *BasicJob) GetID() string {
	return b.ID
}
func (b *BasicJob) SetID(ID string) {
	b.ID = ID
}

func (b *BasicJob) GetType() string {
	return b.Type
}

func (b *BasicJob) GetUser() string {
	return b.User
}

func (b *BasicJob) GetArgs() []string {
	return b.Args
}

func (b *BasicJob) GetParent() string {
	return b.Parent
}

func (b *BasicJob) GetStatus() Status {
	return b.Status
}

func (b *BasicJob) Update(state Status, progress float64) {
	b.Status = state
}

func (b *BasicJob) Save() {
	out, err := json.Marshal(b)
	if err != nil {
		log.Print("marshal BasicJob error", err.Error())
		return
	}
	saveToEtcd("/job/"+normalizeID(b.ID), string(out))
}

type BatchJob struct {
	BasicJob
	BatchArgs   [][]string
	DependJob   string
	DependTasks []int
}

func (b *BatchJob) GetArgs() []string {
	return b.BatchArgs[0]
}

type TrunkJob struct {
	BasicJob
	Trunk       int
	BatchArgs   [][]string
	DependJob   string
	DependTasks []int
}

func (b *TrunkJob) GetArgs() []string {
	return b.BatchArgs[0]
}

type MultipleJob struct {
	BasicJob
	Input      int
	JobNumber  int
	OptionFile string
}

type ServiceJob struct {
	BasicJob
	sync.Mutex
	TID       string
	Step      int
	Progress  float64
	UpdateURL string
}

type JobUpdate struct {
	ID       string
	Progress float64
}

var serviceStatusMap = map[Status]string{
	Waiting: "Pending",
	Running: "Running",
	Success: "Done",
	Failure: "Failed",
	Killed:  "Stopped",
}

func (s *ServiceJob) Save() {
	out, err := json.Marshal(s)
	if err != nil {
		log.Print("marshal ServiceJob error", err.Error())
		return
	}
	saveToEtcd("/job/"+normalizeID(s.ID), string(out))
}

func (s *ServiceJob) Update(state Status, progress float64) {
	s.Lock()
	defer s.Unlock()
	if state != Preserve {
		s.Status = state
	}
	if s.Progress > progress {
		return
	}
	s.Progress = progress
	values := url.Values{
		"tid":      {s.TID},
		"step":     {strconv.Itoa(s.Step)},
		"progress": {strconv.FormatFloat(s.Progress, 'f', 2, 64)},
		"state":    {serviceStatusMap[s.Status]},
	}
	log.Printf("update service %s %d %f %s", s.TID, s.Step, s.Progress, serviceStatusMap[s.Status])
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	_, err := client.PostForm(s.UpdateURL, values)
	if err != nil {
		log.Print("Error: post update to %s failed %v", s.UpdateURL, err)
	}
}
