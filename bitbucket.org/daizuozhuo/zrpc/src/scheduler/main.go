package main

import (
	"log"
	"net"
	"os"
	"time"

	"bitbucket.org/daizuozhuo/zrpc/src/config"
	"bitbucket.org/daizuozhuo/zrpc/src/protocol"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

type Scheduler struct {
	executor *mesos.ExecutorInfo
	tasks    []protocol.Task
}

func newScheduler(exec *mesos.ExecutorInfo) *Scheduler {
	return &Scheduler{executor: exec}
}

func (sched *Scheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Print("Framework Registered with Master ", masterInfo)
}

func (sched *Scheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Print("Framework Registered with Master ", masterInfo)
}

func (sched *Scheduler) Disconnected(driver sched.SchedulerDriver) {

}
func (sched *Scheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	for _, offer := range offers {
		cpus := 0.0
		mems := 0.0
		for _, res := range offer.Resources {
			if res.GetName() == "cpus" {
				cpus += res.GetScalar().GetValue()
			}
			if res.GetName() == "mem" {
				mems += res.GetScalar().GetValue()
			}
		}
		log.Print("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems)
		CPUS_PER_TASK := 1
		MEM_PER_TASK := 0
		var tasks []*mesos.TaskInfo
		for len(sched.tasks) > 0 && cpus >= CPUS_PER_TASK {
			taskInfo := sched.tasks[0].TaskInfo()
			sched.tasks = sched.tasks[1:]
			taskId := &mesos.TaskID{Value: taskInfo.ID}
			task := &mesos.TaskInfo{
				Name:     proto.String("go-task-" + taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: sched.executor,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", CPUS_PER_TASK),
					util.NewScalarResource("mem", MEM_PER_TASK),
				},
			}
			log.Printf("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			cpus -= CPUS_PER_TASK
			mems -= MEM_PER_TASK
		}
		log.Print("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(5)})
	}
}

func (sched *Scheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Println("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
}

func (sched *Scheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Printf("offer rescinded: %v", oid)
}
func (sched *Scheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Printf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (sched *Scheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Printf("slave lost: %v", sid)
}
func (sched *Scheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Printf("executor %q lost on slave %q code %d", eid, sid, code)
}
func (sched *Scheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Printf("Scheduler received error: %v", err)
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}

func main() {
	frameworkInfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("zrpc"),
	}
	scheduler := newScheduler(nil)
	zrpcConfig := config.GetConfig()
	driverConfig := sched.DriverConfig{
		Scheduler:      scheduler,
		Framework:      frameworkInfo,
		Master:         zrpcConfig.Master,
		BindingAddress: parseIP(zrpcConfig.IP),
	}
	driver, err := sched.NewMesosSchedulerDriver(driverConfig)
	if err != nil {
		log.Fatal("unable to create schedulerDriver", err.Error())
	}
	if stat, err := driver.Run(); err != nil {
		log.Printf("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
		time.Sleep(2 * time.Second)
		os.Exit(1)
	}
	log.Print("framework terminating")
}
