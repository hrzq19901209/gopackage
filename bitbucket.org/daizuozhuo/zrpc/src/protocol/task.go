package protocol

import (
	"fmt"
	"log"
	"strings"
	"time"
)

type Constraint struct {
	GPU       bool
	Threads   int
	PreferIP  string
	ExcludeIP string
}

type Task interface {
	Stop()
	Run(*Member, chan error)
	Recover(*Member) error
	Delete(*Member) error
	TaskInfo() *BasicTask
	Span() time.Duration
}

// Multiple tasks compose a job
type BasicTask struct {
	Index    int
	ID       string
	IP       string
	PreferIP string
	Args     []string
	Start    time.Time
	End      time.Time

	MemberName string
	Reply      string
	Status
}

func NewBasicTask(ID string, Index int, cmds []string) *BasicTask {
	Cmds := make([]string, len(cmds))
	copy(Cmds, cmds)
	return &BasicTask{
		ID:     ID,
		Index:  Index,
		Args:   Cmds,
		Start:  time.Now(),
		Status: Waiting,
	}
}

func (t *BasicTask) Span() time.Duration {
	switch t.Status {
	case Waiting:
		return time.Duration(0)
	case Running:
		return time.Since(t.Start)
	default:
		return t.End.Sub(t.Start)
	}
}
func (t *BasicTask) TaskInfo() *BasicTask {
	return t
}

func (t *BasicTask) Run(m *Member, taskError chan error) {
	t.Reply = ""
	t.IP = m.IP
	t.MemberName = m.Name
	var reply Reply
	log.Printf("%d %s: %v", t.Index, m.Name, t.Args)
	t.Status = Running
	t.Start = time.Now()
	err := safeCall(m, "Worker.DoJob", t, &reply)
	t.End = time.Now()
	replyMsg := fmt.Sprintf("%d %s reply: %s %v", t.Index, m.Name, reply.Msg, t.End.Sub(t.Start))
	if err != nil {
		replyMsg = replyMsg + err.Error()
		t.Reply = reply.Msg + err.Error()
	}
	log.Println(replyMsg)
	taskError <- err
}

func (t *BasicTask) Recover(m *Member) error {
	log.Printf("recover %d %s: %v", t.Index, m.Name, t.Args)
	return safeCall(m, "Worker.RecoverJob", t, &Reply{})
}

func (t *BasicTask) Delete(m *Member) error {
	return safeCall(m, "Worker.DeleteJob", t.ID, &Reply{})
}

func (t *BasicTask) Stop() {
	t.Status = Killed
	retry := true
	for retry {
		retry = false
		err := call(t.IP, "Worker.StopJob", t.ID, &Reply{})
		if err != nil {
			log.Printf("stop job %v on %s error %v", t.Args, t.MemberName, err)
			retry = true
		}
	}
}

// Clonable Task can be replicated to other machine
type ClonableTask struct {
	*BasicTask
	Output int
	Shadow *ClonableTask
}

func NewClonableTask(ID string, Index int, cmds []string,
	output int) *ClonableTask {
	task := NewBasicTask(ID, Index, cmds)
	return &ClonableTask{
		BasicTask: task,
		Output:    output,
	}
}

func (c *ClonableTask) StartShadow(m *Member, taskError chan error) {
	taskID := c.BasicTask.ID + "shadow"
	t := NewClonableTask(taskID, c.Index, c.Args, c.Output)
	c.Shadow = t

	go func() {
		var reply Reply
		log.Printf("'%d %s: %s", t.Index, m.Name, strings.Join(t.Args, " "))
		t.Status = Running
		t.Start = time.Now()
		err := safeCall(m, "Worker.DoShadowJob", t, &reply)
		taskError <- err
		t.End = time.Now()
		replyMsg := fmt.Sprintf("'%d %s reply: %s %v", t.Index, m.Name, reply.Msg, t.End.Sub(t.Start))
		if err != nil {
			replyMsg = replyMsg + err.Error()
			t.Reply = reply.Msg + err.Error()
		}
		log.Println(replyMsg)
	}()
}

type TrunkTask struct {
	*BasicTask
	Parallelism int
	BatchArgs   [][]string
}

func NewTrunkTask(ID string, Index int, cmds [][]string, p int) *TrunkTask {
	task := NewBasicTask(ID, Index, []string{})
	return &TrunkTask{
		BasicTask:   task,
		Parallelism: p,
		BatchArgs:   cmds,
	}
}

func (t *TrunkTask) Run(m *Member, taskError chan error) {
	t.Reply = ""
	errorChan := make(chan error, len(t.BatchArgs))
	t.Status = Running
	bound := make(chan int, t.Parallelism)
	for i := 0; i < t.Parallelism; i++ {
		bound <- i
	}
	for _, args := range t.BatchArgs {
		<-bound
		cmds := make([]string, len(args))
		copy(cmds, args)
		basicTask := t.BasicTask
		basicTask.Args = cmds
		go func() {
			basicTask.Run(m, errorChan)
			bound <- 1
		}()
	}
	for i := 0; i < len(t.BatchArgs); i++ {
		err := <-errorChan
		if err != nil {
			taskError <- err
		}
	}
	taskError <- nil
}

type ServiceTask struct {
	*BasicTask
	PID      string
	Progress float64
	LogURL   string
}

type ServiceReply struct {
	Ok     bool
	Msg    string
	LogURL string
}

func NewServiceTask(ID string, cmds []string) *ServiceTask {
	Cmds := make([]string, len(cmds))
	copy(Cmds, cmds)
	task := NewBasicTask(ID, 0, Cmds)
	return &ServiceTask{
		BasicTask: task,
		Progress:  0,
	}
}

func (t *ServiceTask) Run(m *Member, taskError chan error) {
	t.Reply = ""
	t.IP = m.IP
	t.MemberName = m.Name
	var reply ServiceReply
	log.Printf("%d %s: %s", t.Index, m.Name, strings.Join(t.Args, " "))
	t.Status = Running
	t.Start = time.Now()
	err := safeCall(m, "Worker.DoServiceJob", t, &reply)
	t.End = time.Now()

	replyMsg := fmt.Sprintf("%d %s reply: %s %v", t.Index, m.Name, reply.Msg, t.End.Sub(t.Start))
	if err != nil {
		replyMsg = replyMsg + err.Error()
		t.Reply = reply.Msg + err.Error()
	}
	t.LogURL = reply.LogURL
	log.Println(replyMsg)
	taskError <- err
}
