package main

import (
	"flag"
	"github.com/gogo/protobuf/proto"
	"net"
	"os"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	. "github.com/mesosphere/mesos-framework-tutorial/scheduler"
	. "github.com/mesosphere/mesos-framework-tutorial/server"
)

const (
	CPUS_PER_TASK       = 1
	MEM_PER_TASK        = 128
	defaultArtifactPort = 12345
	defaultImage        = "http://www.gabrielhartmann.com/Things/Plants/i-W2N2Rxp/0/O/DSCF6636.jpg"
)

var (
	address      = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	artifactPort = flag.Int("artifactPort", defaultArtifactPort, "Binding port for artifact server")
	master       = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	executorPath = flag.String("executor", "./example_executor", "Path to test executor")
)

func init() {
	flag.Parse()
}

func main() {
	uri := ServeExecutorArtifact(*address, *artifactPort, *executorPath)

	exec := prepareExecutorInfo(uri, getExecutorCmd(*executorPath))

	scheduler, err := NewExampleScheduler(exec, CPUS_PER_TASK, MEM_PER_TASK)
	if err != nil {
		log.Fatalf("Failed to create scheduler with error: %v\n", err)
		os.Exit(-2)
	}

	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Test Framework (Go)"),
	}

	config := sched.DriverConfig{
		Scheduler:      scheduler,
		Framework:      fwinfo,
		Master:         *master,
		Credential:     (*mesos.Credential)(nil),
		BindingAddress: parseIP(*address),
	}

	driver, err := sched.NewMesosSchedulerDriver(config)

	if err != nil {
		log.Fatalf("unable to create a SchedulerDriver: %v\n", err.Error())
		os.Exit(-3)
	}

	if stat, err := driver.Run(); err != nil {
		log.Fatalf("framework stopped with status %s and error: %s\n", stat.String(), err.Error())
		os.Exit(-4)
	}
}

func prepareExecutorInfo(uri string, cmd string) *mesos.ExecutorInfo {
	executorUris := []*mesos.CommandInfo_URI{
		{
			Value:      &uri,
			Executable: proto.Bool(true),
		},
	}

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor (GO)"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String(cmd),
			Uris:  executorUris,
		},
	}
}

func getExecutorCmd(path string) string {
	return "." + GetHttpPath(path)
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}

	if len(addr) < 1 {
		log.Fatal("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}
