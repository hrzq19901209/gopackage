package protocol

import (
	"fmt"
	"html/template"
	"log"
	"strconv"
	"strings"
	"time"

	"bitbucket.org/daizuozhuo/zrpc/src/graph"
)

type Master struct {
	Job

	ID       string
	Start    time.Time
	End      time.Time
	Finished chan bool
	Kill     chan bool
	Tasks    []Task
	NDone    int
	Depend   *Master
	Con      *Constraint

	manager *Manager
	taskIPs []string
}

func NewMaster(m *Manager, job Job, id string) *Master {
	return &Master{
		Job:      job,
		NDone:    0,
		ID:       id,
		Finished: make(chan bool, 1),
		Kill:     make(chan bool, 1),
		manager:  m,
	}
}

func (m *Master) HTML() template.HTML {
	var out []string
	skip := false
	if len(m.Tasks) > 1000 {
		skip = true
	}
	for i, task := range m.Tasks {
		info := task.TaskInfo()
		if info.Status == Success && skip && i < len(m.Tasks)-100 {
			continue
		}
		msg := fmt.Sprintf("%d %s: %v %v Status: %v Reply: %s", info.Index, info.MemberName,
			info.Args, info.Span(), info.Status, info.Reply)
		out = append(out, msg)
	}
	return template.HTML(strings.Join(out, "<br/>"))
}

func (m *Master) StatusMsg() string {
	var status string
	switch m.GetStatus() {
	case Failure:
		status = fmt.Sprintf("Failed, From %s to %s, takes %s",
			m.Start.Format(time.Stamp),
			m.End.Format(time.Stamp),
			m.End.Sub(m.Start).String())
	case Success:
		status = fmt.Sprintf("Success, From %s to %s, takes %s",
			m.Start.Format(time.Stamp),
			m.End.Format(time.Stamp),
			m.End.Sub(m.Start).String())
	case Killed:
		status = fmt.Sprintf("Killed, From %s to %s, takes %s",
			m.Start.Format(time.Stamp),
			m.End.Format(time.Stamp),
			m.End.Sub(m.Start).String())
	case Running:
		var percent int = 0
		if len(m.Tasks) != 0 {
			percent = m.NDone * 100 / len(m.Tasks)
		}
		status = fmt.Sprintf("%d%% %s to %s, elapse %s",
			percent,
			m.Start.Format(time.Stamp),
			time.Now().Format(time.Stamp),
			time.Now().Sub(m.Start).String())
	case Waiting:
		status = fmt.Sprintf("Waiting...%v", m.Job.GetArgs())

	}
	return status
}

func (m *Master) sendTask(task Task, con *Constraint, mapChan chan int) {
	c := *con
	taskInfo := task.TaskInfo()
	c.PreferIP = taskInfo.PreferIP
	retry := true
	times := 0

	for retry {
		times += 1
		retry = false
		member, err := m.manager.RequestMember(&c)
		if err != nil {
			log.Println("Request Member error", err)
			mapChan <- -1
			return
		}
		taskError := make(chan error)
		go task.Run(member, taskError)
	SendTaskError:
		gluster := true
		select {
		case <-m.Kill:
			task.Stop()
		case re := <-taskError:
			if re == nil {
				if taskInfo.Status != Killed {
					mapChan <- taskInfo.Index
					taskInfo.Status = Success
				}
				break
			}
			errMsg := re.Error()
			switch {
			case strings.HasPrefix(errMsg, "GlusterError"):
				gluster = false
				retry = true
				time.Sleep(time.Second * 3)
				time.Sleep(time.Second * 3)
			case strings.HasPrefix(errMsg, "RPCError"):
				if times < 3 {
					retry = true
					time.Sleep(time.Second * 3)
				} else {
					mapChan <- -1
				}
			case strings.HasPrefix(errMsg, "Killed"):
				mapChan <- -2
			case strings.HasPrefix(errMsg, "DialError"):
				retry = true
				time.Sleep(time.Second * 3)
			case strings.HasPrefix(errMsg, "ConnectError"):
				retry = true
			default:
				//other unknown Error, maybe connection fail
				log.Print("Unkown Error:", errMsg)
				for member.InGroup {
					err = task.Recover(member)
					if err != nil && strings.HasPrefix(err.Error(), "DialError") {
						log.Print("Recover Task", err.Error())
						time.Sleep(time.Second * 10)
					} else {
						taskError <- err
						goto SendTaskError
					}
				}
				retry = true
			}
		}
		task.Delete(member)
		m.manager.SetMemberGluster(member, gluster)
		m.manager.ReleaseMember(member, &c)
	}
}

func (m *Master) drainTasks(mapChan chan int, n int) error {
	for i := 0; i < n; i++ {
		select {
		case <-m.Kill:
			return nil
		case r := <-mapChan:
			if r == -1 {
				m.fail()
				return nil
			}
		}
	}
	m.finish()
	return nil
}

func (m *Master) drainClonableTasks(tasks []*ClonableTask, mapChan chan int, shadowChan chan int) error {
	N := len(tasks)
	for i := 0; i < N; {
		select {
		case <-m.Kill:
			return nil
		case r := <-mapChan:
			if r == -1 {
				m.fail()
				return nil
			}
			if r >= 0 {
				if tasks[r].Shadow != nil {
					log.Println("stop shadow job", tasks[r].Index)
					tasks[r].Shadow.Stop()
				}
				m.taskIPs[r] = tasks[r].IP
				i++
			}
		case r := <-shadowChan:
			if r >= 0 {
				log.Println("stop origin job", tasks[r].Index)
				tasks[r].Stop()
				m.taskIPs[r] = tasks[r].Shadow.IP
				i++
			}
		}
	}
	return nil
}

func (m *Master) runTasks(tasks []Task, c *Constraint, mapChan chan int) error {
	m.taskIPs = make([]string, len(tasks))
	totalCPU := m.manager.TotalCPU()
	if c.Threads > 0 {
		totalCPU = totalCPU / c.Threads
	}
	bound := make(chan int, totalCPU)
	for i := 0; i < totalCPU; i++ {
		bound <- i
	}

	for i := 0; i < len(tasks); i++ {
		select {
		case <-m.Kill:
			return nil
		case <-bound:
			go func(index int) {
				m.sendTask(tasks[index], c, mapChan)
				m.taskIPs[index] = tasks[index].TaskInfo().IP
				m.NDone += 1
				bound <- 1
			}(i)
		}
	}
	return nil
}

func (m *Master) speculateTasks(tasks []*ClonableTask, c *Constraint, mapChan chan int) {
	time.Sleep(time.Second)
	for {
		var mark *ClonableTask = nil
		var index int = -1
		var span float64 = -1.0
		for i, task := range tasks {
			if task.Status != Success &&
				task.Shadow == nil &&
				span < time.Now().Sub(task.Start).Minutes() {
				mark = task
				index = i
			}
		}
		if mark == nil {
			return
		}
		c.ExcludeIP = mark.IP
		member, err := m.manager.RequestMember(c)

		if err != nil {
			log.Println("Error: Requesst Member error", err)
			return
		}

		//the original task is finished now
		if mark.Status == Success {
			m.manager.ReleaseMember(member, c)
			continue
		}

		//run selected task
		taskError := make(chan error)
		mark.StartShadow(member, taskError)
		go func() {
			select {
			case <-m.Kill:
				mark.Shadow.Stop()
			case re := <-taskError:
				if re == nil {
					if mark.Shadow.Status != Killed {
						mapChan <- index
						mark.Shadow.Status = Success
						err := call(member.IP, "Worker.CollectShadowResult", mark.Shadow, &Reply{})
						if err != nil {
							log.Println("Error: collect shadow result", mark.ID, err)
						}
					}
				} else {
					errMsg := re.Error()
					if strings.HasPrefix(errMsg, "Killed") {
						mapChan <- -2
					} else {
						mapChan <- -1
					}
				}
			}
			m.manager.ReleaseMember(member, c)
		}()
	}
}

func (m *Master) Run() error {
	if m.GetStatus() == Killed {
		return nil
	}
	var err error
	m.Update(Running, 0)
	switch j := m.Job.(type) {
	case *TrunkJob:
		err = m.RunTrunk(j, m.Depend)
	case *MultipleJob:
		err = m.RunMultiple(j)
	case *BatchJob:
		err = m.RunBatch(j, m.Depend)
	case *BasicJob:
		err = m.RunSingle(j)
	case *ServiceJob:
		err = m.RunService(j)
	default:
		err = fmt.Errorf("unrecgonized job type %s", m.Job.GetType())
	}
	info := fmt.Sprintf("%v %v %d %v",
		m.GetStatus(), m.GetArgs(), len(m.Tasks), m.End.Sub(m.Start))
	saveToEtcd(m.ID, info)
	return err
}

func (m *Master) RunTrunk(job *TrunkJob, depend *Master) error {
	job.ID = m.ID
	args := job.BatchArgs
	m.Start = time.Now()
	m.NDone = 0
	constraint := &Constraint{Threads: -1, GPU: job.GPU}
	m.Con = constraint
	parallelism := 32
	if job.Threads > 0 {
		parallelism /= job.Threads
	}
	var tasks []Task
	for i := 0; i < len(args); i += job.Trunk {
		taskID := job.ID + strconv.Itoa(i)
		task := NewTrunkTask(taskID, i, args[i:i+job.Trunk], parallelism)
		if depend != nil &&
			job.DependTasks[i] < len(depend.taskIPs) {
			task.PreferIP = depend.taskIPs[job.DependTasks[i]]
		}
		tasks = append(tasks, task)
	}
	m.Tasks = tasks
	mapChan := make(chan int, len(tasks))
	m.runTasks(tasks, constraint, mapChan)
	go m.drainTasks(mapChan, len(tasks))
	return nil
}

func (m *Master) RunBatch(job *BatchJob, depend *Master) error {
	job.ID = m.ID
	args := job.BatchArgs
	output := job.Cache
	m.Start = time.Now()
	m.NDone = 0

	constraint := &Constraint{Threads: job.Threads, GPU: job.GPU}
	m.Con = constraint
	var tasks []Task
	outputIndex := -1
	if len(output) == 1 && output[0] > 0 {
		outputIndex = output[0]
		for i := 0; i < len(args); i++ {
			taskID := job.ID + strconv.Itoa(i)
			task := NewClonableTask(taskID, i, args[i], outputIndex)
			if depend != nil &&
				job.DependTasks[i] < len(depend.taskIPs) {
				task.PreferIP = depend.taskIPs[job.DependTasks[i]]
			}
			tasks = append(tasks, task)
		}
	} else {
		for i := 0; i < len(args); i++ {
			taskID := job.ID + strconv.Itoa(i)
			task := NewBasicTask(taskID, i, args[i])
			if depend != nil &&
				job.DependTasks[i] < len(depend.taskIPs) {
				task.PreferIP = depend.taskIPs[job.DependTasks[i]]
			}
			tasks = append(tasks, task)
		}
	}
	m.Tasks = tasks
	mapChan := make(chan int, len(tasks))
	m.runTasks(tasks, constraint, mapChan)
	if outputIndex > 0 {
		var ctasks []*ClonableTask
		for _, task := range tasks {
			ctasks = append(ctasks, task.(*ClonableTask))
		}
		shadowChan := make(chan int, len(tasks))
		go m.speculateTasks(ctasks, constraint, shadowChan)
		go m.drainClonableTasks(ctasks, mapChan, shadowChan)
	} else {
		go m.drainTasks(mapChan, len(tasks))
	}
	return nil
}

func (m *Master) RunMultiple(job *MultipleJob) error {
	job.ID = m.ID
	output := job.Cache
	m.Start = time.Now()
	m.NDone = 0
	if job.JobNumber <= 0 {
		job.JobNumber = m.manager.NumMember()
	}
	fileName := job.Args[job.Input]
	var files []string
	var err error
	files, err = graph.Split(fileName, job.JobNumber)
	if err != nil {
		m.fail()
		return err
	}
	constraint := &Constraint{Threads: job.Threads, GPU: job.GPU}
	m.Con = constraint
	var outputIndex = -1
	if len(output) == 1 && output[0] > 0 {
		outputIndex = output[0]
	}
	var tasks []Task
	var task Task
	for i := 0; i < len(files); i++ {
		taskID := job.ID + strconv.Itoa(i)
		job.Args[job.Input] = files[i]
		if outputIndex > 0 {
			task = NewClonableTask(taskID, i, job.Args, outputIndex)
		} else {
			task = NewBasicTask(taskID, i, job.Args)
		}
		tasks = append(tasks, task)
	}
	mapChan := make(chan int, len(tasks))
	m.Tasks = tasks
	m.runTasks(tasks, constraint, mapChan)
	if outputIndex > 0 {
		var ctasks []*ClonableTask
		for _, task := range tasks {
			ctasks = append(ctasks, task.(*ClonableTask))
		}
		shadowChan := make(chan int, len(tasks))
		go m.speculateTasks(ctasks, constraint, shadowChan)
		go m.drainClonableTasks(ctasks, mapChan, shadowChan)
	} else {
		go m.drainTasks(mapChan, len(tasks))
	}
	return nil
}

func (m *Master) RunService(job *ServiceJob) error {
	job.ID = m.ID
	m.Start = time.Now()
	mapChan := make(chan int, 1)
	constraint := &Constraint{Threads: job.Threads, GPU: job.GPU}
	m.Con = constraint
	task := NewServiceTask(m.ID, job.Args)
	m.Tasks = append(m.Tasks, task)
	m.runTasks(m.Tasks, constraint, mapChan)
	go m.drainTasks(mapChan, 1)
	return nil
}

func (m *Master) RunSingle(job *BasicJob) error {
	job.ID = m.ID
	m.Start = time.Now()
	mapChan := make(chan int, 1)
	constraint := &Constraint{Threads: job.Threads, GPU: job.GPU}
	m.Con = constraint
	task := NewBasicTask(m.ID, 0, job.Args)
	m.Tasks = append(m.Tasks, task)
	m.runTasks(m.Tasks, constraint, mapChan)
	go m.drainTasks(mapChan, 1)
	return nil
}

func (m *Master) Wait() {
	<-m.Finished
	m.Finished <- true
}

func (m *Master) Stop() {
	if m.GetStatus() == Killed {
		return
	}
	m.Update(Killed, 100)
	close(m.Kill)
	m.End = time.Now()
	log.Printf("%s, id %s, tasks %d, time %s, cpu %d, gpu %v, args %v",
		"job result killed",
		m.ID,
		len(m.Tasks),
		m.End.Sub(m.Start).String(),
		m.Con.Threads,
		m.Con.GPU,
		m.GetArgs(),
	)
	m.Save()
	m.Finished <- true
}

func (m *Master) fail() {
	close(m.Kill)
	m.End = time.Now()
	m.Update(Failure, 100)
	log.Printf("%s, id %s, tasks %d, time %s, cpu %d, gpu %v, args %v",
		"job result failed",
		m.ID,
		len(m.Tasks),
		m.End.Sub(m.Start).String(),
		m.Con.Threads,
		m.Con.GPU,
		m.GetArgs(),
	)
	m.Save()
	m.Finished <- true
}

func (m *Master) finish() {
	m.End = time.Now()
	m.Update(Success, 100)
	log.Printf("%s, id %s, tasks %d, time %s, cpu %d, gpu %v, args %v",
		"job result finished",
		m.ID,
		len(m.Tasks),
		m.End.Sub(m.Start).String(),
		m.Con.Threads,
		m.Con.GPU,
		m.GetArgs(),
	)
	m.Save()
	m.Finished <- true
}
