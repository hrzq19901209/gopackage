package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"bitbucket.org/daizuozhuo/zrpc/src/config"
	"bitbucket.org/daizuozhuo/zrpc/src/service"
)

var baseURL string
var cmdURL string

func init() {
	_, err := os.Stat("test")
	if err != nil {
		err = os.Mkdir("test", 0777)
		if err != nil {
			fmt.Println("create test dir error", err)
			return
		}
	}
	test_config := config.LoadConfig("../config/test.conf")
	service.LoadService("../service/service.json")
	baseURL = fmt.Sprintf("http://%s:%s/", test_config.MasterIP, test_config.Port)
	cmdURL = fmt.Sprintf("%scmd/", baseURL)
	RunRPCServer(test_config)
	go RunHTTPServer(test_config)
	time.Sleep(1000 * time.Millisecond)
}

func must(e error, s string, t *testing.T) {
	if e != nil {
		t.Error(s, e.Error())
	}
}

func wait(t *testing.T, reader io.Reader) {
	_, err := http.Post(cmdURL+"wait", "application/json", reader)
	must(err, "wait", t)
}

func TestSingle(t *testing.T) {
	url := cmdURL + "create"
	job := `{
		"Priority":"Pro",
		"Type":"single",
		"Job":{"Type": "single", "Args":["sleep", "1s"], "Threads": 1}
	}`
	res, err := http.Post(url, "application/json", bytes.NewBufferString(job))
	if err != nil {
		t.Error(err)
	}
	var reply CmdReply
	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&reply)
	if err != nil {
		t.Error(err)
	}
	if reply.Msg != "Success" {
		t.Error(reply.Msg)
	}
	reader, _ := json.Marshal(reply)
	wait(t, bytes.NewBuffer(reader))
}

func TestBatch(t *testing.T) {
	url := cmdURL + "create"
	defer os.Remove("test/result")

	job := `{
		"Priority":"Pro",
		"Type":"batch",
		"Job":
			{"Type": "batch", "BatchArgs":[
			["bash", "-c", "echo -n 1 >> test/result"],
			["bash", "-c", "echo -n 2 >> test/result"],
			["bash", "-c", "echo -n 3 >> test/result"],
			["bash", "-c", "echo -n 4 >> test/result"]
		], "Threads": 1}
	}`
	res, err := http.Post(url, "application/json", bytes.NewBufferString(job))
	if err != nil {
		t.Error(err)
	}
	wait(t, res.Body)
	result, err := ioutil.ReadFile("test/result")
	if err != nil {
		t.Error(err)
	}
	expect := "1234"
	if len(result) != len(expect) {
		t.Errorf("expect %s, got %s", expect, result)
	}
}

func TestKill(t *testing.T) {
	defer os.Remove("test/result")
	job := `{
		"Priority":"Pro",
		"Type":"batch",
		"Job":
			{"Type": "batch", "BatchArgs":[
			["bash", "-c", "echo -n 1 >> test/result; sleep 1"],
			["bash", "-c", "echo -n 2 >> test/result; sleep 1"],
			["bash", "-c", "echo -n 3 >> test/result; sleep 1"],
			["bash", "-c", "echo -n 4 >> test/result; sleep 1"]
		], "Threads": 2}
	}`
	res, err := http.Post(cmdURL+"create", "application/json", bytes.NewBufferString(job))
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Millisecond * 500)
	res, err = http.Post(cmdURL+"kill", "appplication/json", res.Body)
	if err != nil {
		t.Errorf("kill %v", err)
	}
	_, err = http.Post(cmdURL+"wait", "appplication/json", res.Body)
	if err != nil {
		t.Errorf("wait %v", err)
	}
	result, err := ioutil.ReadFile("test/result")
	if err != nil {
		t.Error(err)
	}
	expect := "12"
	if len(result) != len(expect) {
		t.Errorf("expect %s, got %s", expect, result)
	}
}

func TestService(t *testing.T) {
	service := `{
		"Name":"test",
		"Args": ["go", "run", "test/progress.go", "$seconds"],
		"ArgsType": ["", "", "", "OPTION"]
	}`
	req, err := http.NewRequest("PUT", baseURL+"services", bytes.NewBufferString(service))
	must(err, "put services request", t)
	req.Header.Set("ContentType", "application/json")
	res, err := http.DefaultClient.Do(req)
	must(err, "add service", t)

	rep, err := ioutil.ReadAll(res.Body)
	must(err, "read register", t)

	var reply Message
	must(json.Unmarshal(rep, &reply), "decode register", t)

	if reply.Body["status"] != "success" {
		t.Error(reply.Body["status"])
	}
	data := `{
	    "TID": "testService",
	    "Step": 1,
		"Args": {"seconds": "1"},
		"UpdateURL": "http://127.0.0.1:4000/update-test"
	}`
	finished := make(chan bool, 1)
	go func() {
		handleTest := func(rw http.ResponseWriter, req *http.Request) {
			req.ParseForm()
			step := req.FormValue("step")
			progress := req.FormValue("progress")
			state := req.FormValue("state")
			t.Log(step, progress, state)
			if state == "Done" {
				finished <- true
			}
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/update-test", handleTest)
		server := &http.Server{
			Addr:    ":4000",
			Handler: mux,
		}
		must(server.ListenAndServe(), "update server", t)
	}()
	res, err = http.Post(baseURL+"service/test", "application/json", bytes.NewBufferString(data))
	must(err, "test", t)
	must(json.NewDecoder(res.Body).Decode(&reply), "decode test", t)
	if reply.Body["status"] != "success" {
		t.Error(reply.Body["status"])
	}
	<-finished
}
