package scheduler

import (
	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"strconv"
)

type ExampleScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
	images        []string
	cpuPerTask    float64
	memPerTask    float64
}

func NewExampleScheduler(exec *mesos.ExecutorInfo, cpuPerTask float64, memPerTask float64) (*ExampleScheduler, error) {
	images, err := readLines("images")
	if err != nil {
		log.Errorf("Failed to read image list with error: %v\n", err)
		return nil, err
	}

	return &ExampleScheduler{
		executor:      exec,
		tasksLaunched: 0,
		totalTasks:    len(images),
		images:        images,
		cpuPerTask:    cpuPerTask,
		memPerTask:    memPerTask,
	}, nil
}

func (sched *ExampleScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
}

func (sched *ExampleScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	logOffers(offers)

	if sched.tasksLaunched >= sched.totalTasks {
		return
	}

	for _, offer := range offers {
		remainingCpus := getOfferCpu(offer)
		remainingMems := getOfferMem(offer)

		var tasks []*mesos.TaskInfo
		for sched.cpuPerTask <= remainingCpus &&
			sched.memPerTask <= remainingmems &&
			sched.taskslaunched < sched.totalTasks {

			log.Infof("Processing image %v of %v\n", sched.tasksLaunched, sched.totalTasks)
			fileName := sched.images[sched.taskslaunched]
			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

			task := &mesos.TaskInfo{
				Name:     proto.String("go-task-" + taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: sched.executor,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpu", sched.cpuPerTask),
					util.NewScalarResource("mem", sched.memPerTask),
				},
				Data: []byte(fileName),
			}
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.getValue())

			tasks = append(tasks, task)
			remainingCpus -= sched.cpuPerTask
			remainingMem -= sched.memPerTask
		}
		log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (sched *ExampleScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
		log.Infof("%v of %v tasks finished.", sched.tasksFinished, sched.totalTasks)
	}

	if sched.tasksFinished >= sched.totalTasks {
		log.Infoln("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.getState() == mesos.TaskState_TASK_FAILED {
		log.Infoln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
		driver.Abort()
	}
}

func (sched *ExampleScheduler) OfferRescinded(s sched.SchedulerDriver, id *mesos.OfferID) {
	log.Infof("Offer '%v' rescinded.\n", *id)
}

func (sched *ExampleScheduler) FrameworkMessage(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, msg string) {
	log.Infof("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (sched *ExampleScheduler) SlaveLost(s sched.SchedulerDriver, id *mesos.SlaveID) {
	log.Infof("Slave '%v' lost.\n", *id)
}

func (sched *ExampleScheduler) ExecutorLost(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	log.Infof("Executor '%v' lost  on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
}

func (sched *ExampleScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}
