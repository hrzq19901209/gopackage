package protocol

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"bitbucket.org/daizuozhuo/zrpc/src/config"
	"time"
)

var manager *Manager

func init() {
	config := config.LoadConfig("../config/config_test.json")
	manager = NewManager(config)
	manager.AddWorker(&WorkerInfo{
		Name: "localhost",
		IP:   "127.0.0.1",
		CPU:  1,
		GPU:  false,
	})
	manager.ListenRPC()
}

func TestWorkStatus(t *testing.T) {
	//test idle status
	status := manager.Status()
	for _, worker := range manager.members {
		expect := fmt.Sprintf("%s : used/total 0/%d Live", worker.Name, worker.NCPU)
		info := status.MemberInfos[0].Info
		if status.MemberInfos[0].Info != info {
			t.Errorf("expect %s, got status %s", expect, info)
		}

		//test busy status
		master := manager.GetMaster("test")
		jobArgs := &JobArgs{
			Args:    []string{"sleep", "1s"},
			Threads: 1,
		}
		go master.RunSingle(jobArgs)
		time.Sleep(100 * time.Millisecond)
		status = manager.Status()
		expect = fmt.Sprintf("%s : used/total 1/%d Live GPU is not supported", worker.Name, worker.NCPU)
		info = status.MemberInfos[0].Info
		if info != expect {
			t.Errorf("expect %s, got status %s", expect, info)
		}
	}
}

func TestMapReduce(t *testing.T) {
	master := manager.GetMaster("test")
	cmd := "go run test/test.go test/input test/"
	defer os.Remove("test/result")
	defer os.Remove("test/input-0")
	defer os.Remove("test/input-1")
	args := strings.Split(cmd, " ")
	jobArgs := &JobArgs{
		Args:    args,
		Threads: -1,
	}
	err := master.RunMapreduce(jobArgs, 3, 4)
	if err != nil {
		t.Errorf("%v", err)
	}
	inputData, err := ioutil.ReadFile("test/input")
	if err != nil {
		t.Errorf("%v", err)
	}
	outputData, err := ioutil.ReadFile("test/result")
	if err != nil {
		t.Errorf("%v", err)
	}
	if len(inputData) != len(outputData) {
		t.Errorf("expect %s, got %s", inputData, outputData)
	}
}
