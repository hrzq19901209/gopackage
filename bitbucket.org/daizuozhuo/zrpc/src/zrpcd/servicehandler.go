package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"bitbucket.org/daizuozhuo/zrpc/src/config"
	"bitbucket.org/daizuozhuo/zrpc/src/protocol"
	"bitbucket.org/daizuozhuo/zrpc/src/service"
)

func response(rw http.ResponseWriter, message []byte, code int) {
	rw.WriteHeader(code)
	rw.Write([]byte(message))
}

func sendState(rw http.ResponseWriter, state string) {
	msg := MakeMessage("states")
	msg.Body["status"] = state
	out, _ := json.Marshal(msg)
	response(rw, out, 200)
}

//service args map
type ServiceData struct {
	TID       string
	Step      int
	UpdateURL string
	Args      map[string]string
}

//a list of service args map
type ServicesData struct {
	TID       string
	UpdateURL string
	Services  []struct {
		Name string
		Step int
		Args map[string]string
	}
}

func stopServicesHandler(rw http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	tid := req.FormValue("tid")
	pidSet := make(map[string]bool)

	//stop services with pid
	for _, master := range GlobalMasters.data {
		if s, ok := master.Job.(*protocol.ServiceJob); ok && s.TID == tid {
			pidSet[s.GetID()] = true
			master.Stop()
		}
	}

	//stop the services's child job
	for _, master := range GlobalMasters.data {
		pid := master.Job.GetParent()
		if pid == "" {
			continue
		}
		if _, found := pidSet[pid]; found {
			master.Stop()
		}
	}
	sendState(rw, "success")
}

func uploadServicesHandler(rw http.ResponseWriter, req *http.Request) {
	req.ParseMultipartForm(1 << 30)
	serviceName := req.FormValue("selectService")
	_, ok := (*service.GetServices())[serviceName]
	if !ok {
		log.Print("Error: select service doesn't exist")
		return
	}
	file, _, err := req.FormFile("uploadfile")
	if err != nil {
		log.Print("Error upload file", err.Error())
		return
	}
	defer file.Close()
	fileName := fmt.Sprintf("%s-%s.tar", serviceName, time.Now().Format("01-02:15:04:05"))
	savePath := filepath.Join(config.GetConfig().StaticDir, fileName)
	f, err := os.OpenFile(savePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Print("Error save file", err.Error())
	}
	defer f.Close()
	io.Copy(f, file)
}

func servicesHandler(rw http.ResponseWriter, req *http.Request) {
	services := service.GetServices()
	if req.Method == "GET" {
		out, _ := json.Marshal(services)
		rw.Write(out)
		return
	} else if req.Method == "PUT" {
		var s service.Service
		err := json.NewDecoder(req.Body).Decode(&s)
		if err != nil {
			sendState(rw, "register service failed "+err.Error())
			return
		}
		services := service.GetServices()
		(*services)[s.Name] = &s
		sendState(rw, "success")
		return
	}
	var data ServicesData
	err := json.NewDecoder(req.Body).Decode(&data)
	if err != nil {
		sendState(rw, "decode services data failed "+err.Error())
		return
	}

	var jobs []protocol.Job
	for _, d := range data.Services {
		s, ok := (*services)[d.Name]
		if !ok {
			sendState(rw, "service not exists: "+d.Name)
		}
		args, err := s.Bind(d.Args)
		if err != nil {
			sendState(rw, "bind service data failed "+err.Error())
			return
		}
		job := &protocol.ServiceJob{
			BasicJob: protocol.BasicJob{
				Type:    "service",
				User:    "altizure",
				Args:    args,
				GPU:     false,
				Threads: 0,
			},
			TID:       data.TID,
			Step:      d.Step,
			Progress:  0,
			UpdateURL: data.UpdateURL,
		}
		jobs = append(jobs, job)
	}
	go func() {
		for _, job := range jobs {
			master := manager.AddJob(job, "Pro")
			GlobalMasters.Insert(master)
			//log.Printf("WriteReply: create service success %v", job.GetArgs())
			currentService = job.(*protocol.ServiceJob)
			master.Wait()
			currentService = nil
			if master.GetStatus() != protocol.Success {
				break
			}
		}
	}()
	sendState(rw, "success")
}

func serviceHandler(rw http.ResponseWriter, req *http.Request) {
	name := req.URL.Path
	services := service.GetServices()
	s, ok := (*services)[name]
	if !ok {
		sendState(rw, "service not exists: "+name)
		return
	}
	var data ServiceData
	err := json.NewDecoder(req.Body).Decode(&data)
	if err != nil {
		sendState(rw, "decode service data failed "+err.Error())
		return
	}
	args, err := s.Bind(data.Args)
	if err != nil {
		sendState(rw, "bind service data failed "+err.Error())
		return
	}
	job := &protocol.ServiceJob{
		BasicJob: protocol.BasicJob{
			Type:    "service",
			User:    "altizure",
			Args:    args,
			GPU:     false,
			Threads: 0,
		},
		TID:       data.TID,
		Step:      data.Step,
		Progress:  0,
		UpdateURL: data.UpdateURL,
	}
	master := manager.AddJob(job, "Pro")
	GlobalMasters.Insert(master)
	//log.Printf("WriteReply: create service success %v", job.GetArgs())
	sendState(rw, "success")
}
