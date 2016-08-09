package main

import (
	"encoding/json"
	"log"
	"net/http"

	"bitbucket.org/daizuozhuo/zrpc/src/protocol"
)

type CmdReply struct {
	Id  string
	Msg string
}

type HealthInfo struct {
	NumMember int
	NumCPU    int
	NumGPU    int
}

const (
	Success = "Success"
	Fail    = "Fail"
)

func queryHandler(rw http.ResponseWriter, request *http.Request) {
	var reply CmdReply
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&reply)
	if err != nil {
		log.Print(err)
		return
	}
	master, ok := GlobalMasters.Get(reply.Id)
	if ok {
		rw.Write([]byte(master.StatusMsg()))
	}
}
func htmlHandler(rw http.ResponseWriter, request *http.Request) {
	var reply CmdReply
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&reply)
	if err != nil {
		log.Print(err)
		return
	}
	master, ok := GlobalMasters.Get(reply.Id)
	if ok {
		rw.Write([]byte(master.HTML()))
	}
}

func killHandler(rw http.ResponseWriter, request *http.Request) {
	var reply CmdReply
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&reply)
	if err != nil {
		log.Print(err)
		return
	}
	master, ok := GlobalMasters.Get(reply.Id)
	if ok {
		master.Stop()
		reply.Msg = "ok"
	} else {
		reply.Msg = "job does not exist"
	}
	out, _ := json.Marshal(&reply)
	rw.Write(out)
}

func healthHandler(rw http.ResponseWriter, request *http.Request) {
	reply := &HealthInfo{
		NumCPU:    manager.NumCPU(),
		NumGPU:    manager.NumGPU(),
		NumMember: manager.NumMember(),
	}
	out, _ := json.Marshal(reply)
	rw.Write(out)
}

func waitHandler(rw http.ResponseWriter, request *http.Request) {
	var reply CmdReply
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&reply)
	if err != nil {
		log.Print(err)
		return
	}
	master, ok := GlobalMasters.Get(reply.Id)
	if !ok {
		rw.Write([]byte(Fail))
		return
	}
	master.Wait()
	if master.GetStatus() == protocol.Success {
		rw.Write([]byte(Success))
	} else {
		rw.Write([]byte(Fail))
	}
}

func writeReply(rw http.ResponseWriter, ID string, Msg string) {
	reply := CmdReply{
		Id:  ID,
		Msg: Msg,
	}
	out, _ := json.Marshal(&reply)
	rw.Write(out)
}

func parseJob(Type string, Job json.RawMessage) (job protocol.Job, err error) {
	switch Type {
	case "multiple":
		job = &protocol.MultipleJob{}
	case "batch":
		job = &protocol.BatchJob{}
	case "single":
		job = &protocol.BasicJob{}
	case "trunk":
		job = &protocol.TrunkJob{}
	case "service":
		job = &protocol.ServiceJob{}
	}
	err = json.Unmarshal(Job, job)
	return job, err
}

func createHandler(rw http.ResponseWriter, req *http.Request) {
	job := struct {
		Priority string
		Type     string
		Job      json.RawMessage
	}{}
	err := json.NewDecoder(req.Body).Decode(&job)
	if err != nil {
		log.Printf("Error: decode in queue Job %v", err)
		return
	}
	j, err := parseJob(job.Type, job.Job)
	if err != nil {
		log.Printf("Error: decode protocol.%s %v", job.Type, err)
		return
	}
	master := manager.AddJob(j, job.Priority)
	GlobalMasters.Clear()
	GlobalMasters.Insert(master)
	//log.Printf("WriteReply: create success %v", j.GetArgs())
	writeReply(rw, master.ID, Success)
}

func updateHandler(rw http.ResponseWriter, req *http.Request) {
	var update protocol.JobUpdate
	err := json.NewDecoder(req.Body).Decode(&update)
	if err != nil {
		log.Printf("Error: decode udpate CmdReply %s", err.Error())
	}
	master, ok := GlobalMasters.Get(update.ID)
	if !ok {
		log.Printf("Warn: master %s not exist", update.ID)
		writeReply(rw, update.ID, "not exist")
		return
	}
	master.Update(protocol.Preserve, update.Progress)
	writeReply(rw, update.ID, Success)
}
