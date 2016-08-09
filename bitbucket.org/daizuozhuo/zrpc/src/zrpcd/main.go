package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"bitbucket.org/daizuozhuo/zrpc/src/config"
	"bitbucket.org/daizuozhuo/zrpc/src/protocol"
	"bitbucket.org/daizuozhuo/zrpc/src/service"
	"bitbucket.org/daizuozhuo/zrpc/src/zsystem"
)

var manager *protocol.Manager
var currentService *protocol.ServiceJob
var GlobalMasters = NewMasters()

type Masters struct {
	sync.RWMutex
	data map[string]*protocol.Master
}

func NewMasters() *Masters {
	return &Masters{data: make(map[string]*protocol.Master)}
}

func (m *Masters) Insert(master *protocol.Master) {
	m.Lock()
	defer m.Unlock()
	m.data[master.ID] = master
}

func (m *Masters) Get(id string) (*protocol.Master, bool) {
	m.RLock()
	defer m.RUnlock()
	master, ok := m.data[id]
	return master, ok
}

func (m *Masters) Clear() {
	m.Lock()
	defer m.Unlock()
	if len(m.data) < 100 {
		return
	}

	var keys []string
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i := 0; i < len(keys)/2; i++ {
		status := m.data[keys[i]].GetStatus()
		if status == protocol.Waiting || status == protocol.Running {
			continue
		}
		delete(m.data, keys[i])
	}
}


type Message struct {
	Name string
	Body map[string]string
}

func MakeMessage(name string) *Message {
	msg := new(Message)
	msg.Name = name
	msg.Body = make(map[string]string)
	return msg
}

func MakeAPI(handler http.HandlerFunc, apiurl string) http.HandlerFunc {
	return func(rw http.ResponseWriter, request *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				log.Println("Recovered in API for url", apiurl, r)
			}
		}()
		conf := config.GetConfig()
		if conf.IP != conf.MasterIP {
			redirectURL := fmt.Sprintf("http://%s:%s%s",
				conf.MasterIP, conf.Port, apiurl)
			u, err := url.Parse(redirectURL)
			if err != nil {
				log.Print("parse redirect url error", err)
			}
			request.URL = u
			request.RequestURI = ""
			res, err := http.DefaultClient.Do(request)
			if err != nil {
				log.Print("Redirect request error", err)
				return
			}
			_, err = io.Copy(rw, res.Body)
			if err != nil {
				log.Print("copy response error", err)
			}
			return
		}
		handler(rw, request)
	}
}

func listHandler(w http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	path := request.FormValue("path")
	if path == "" {
		path = "."
	}

	files, _ := ioutil.ReadDir(path)
	msg := MakeMessage("list")
	for i, f := range files {
		msg.Body[string(i)] = f.Name()
	}
	data, err := json.Marshal(msg)
	if err == nil {
		w.Write(data)
	} else {
		fmt.Fprintln(w, err)
	}
}

func createJobHandler(rw http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	args := request.FormValue("job-args")
	log.Println(args)
	user := request.FormValue("user")
	gpu := (request.FormValue("GPU") == "True")
	priority := request.FormValue("priority")
	threads, err := strconv.Atoi(request.FormValue("threads"))
	if err != nil {
		threads = 1
	}
	job := &protocol.BasicJob{
		Type:    "single",
		User:    user,
		Args:    strings.Split(args, " "),
		Threads: threads,
		GPU:     gpu,
	}
	master := manager.AddJob(job, priority)
	GlobalMasters.Insert(master)
	//log.Printf("WriteReply: create job success %v", job.Args)
	writeReply(rw, master.ID, "Success")
}

func killXMSHandler(rw http.ResponseWriter, request *http.Request) {
	cmd := exec.Command("ansible", "all", "-a", "pkill -KILL xms", "--sudo")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("ansible kill xms: %s", err.Error())
	}
	log.Print(string(out))
	rw.Write(out)
}

func statesHandler(rw http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	name := request.FormValue("name")
	ip := request.FormValue("ip")
	status := zsystem.GetStatus()
	msg := new(Message)
	msg.Name = "states"
	msg.Body = map[string]string{
		"name":    name,
		"ip":      ip,
		"cpu":     strconv.FormatFloat(status.CPUUsage, 'f', 2, 64),
		"mem":     strconv.FormatFloat(status.MemoryUsage, 'f', 2, 64),
		"state":   "idle",
		"storage": strconv.FormatFloat(status.HarddiskUsage, 'f', 2, 64),
		"uptime":  strconv.FormatFloat(status.Uptime, 'f', 6, 64),
	}
	if currentService != nil {
		msg.Body["state"] = "busy"
		msg.Body["task"] = currentService.TID
	}
	out, _ := json.Marshal(msg)
	rw.Write(out)
}

type IndexContent struct {
	Masters map[string]*protocol.Master
	Manager *protocol.Manager
	Config  *config.Config
}

func indexHandler(rw http.ResponseWriter, request *http.Request) {
	content := &IndexContent{
		Masters: GlobalMasters.data,
		Manager: manager,
		Config:  config.GetConfig(),
	}

	staticDir := config.GetConfig().StaticDir
	t := template.Must(template.ParseFiles(
		filepath.Join(staticDir, "template/header.html"),
		filepath.Join(staticDir, "template/footer.html"),
		filepath.Join(staticDir, "template/index.html")))
	t.ExecuteTemplate(rw, "main", content)
}

func guideHandler(rw http.ResponseWriter, request *http.Request) {
	staticDir := config.GetConfig().StaticDir
	t := template.Must(template.ParseFiles(
		filepath.Join(staticDir, "template/header.html"),
		filepath.Join(staticDir, "template/footer.html"),
		filepath.Join(staticDir, "template/usage.html")))
	t.ExecuteTemplate(rw, "main", nil)
}

func RunHTTPServer(conf *config.Config) {
	//web page
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/guide", guideHandler)
	http.HandleFunc("/list", listHandler)
	http.HandleFunc("/user/", userHandler)
	http.HandleFunc("/users", usersHandler)
	http.HandleFunc("/createjob", createJobHandler)
	http.HandleFunc("/kill-xms", killXMSHandler)

	//api for zrpckit
	api := map[string]http.HandlerFunc{
		"/cmd/create":    createHandler,
		"/cmd/wait":      waitHandler,
		"/cmd/query":     queryHandler,
		"/cmd/update":    updateHandler,
		"/cmd/kill":      killHandler,
		"/cmd/html":      htmlHandler,
		"/cmd/health":    healthHandler,
		"/states":        statesHandler,
		"/sync-user":     syncUser,
		"/services":      servicesHandler,
		"/services/stop": stopServicesHandler,
		"/services/upload": uploadServicesHandler,
	}
	for url, handler := range api {
		http.HandleFunc(url, MakeAPI(handler, url))
	}

	fs := http.FileServer(http.Dir(conf.StaticDir))
	gluster := http.FileServer(http.Dir("/mnt/gluster"))
	serv := http.HandlerFunc(serviceHandler)
	http.Handle("/service/", http.StripPrefix("/service/", serv))
	http.Handle("/gluster/", http.StripPrefix("/gluster/", gluster))
	http.Handle("/public/", http.StripPrefix("/public/", fs))
	err := http.ListenAndServe(":"+conf.Port, nil)
	if err != nil {
		log.Fatal("Error: ListenAndServe", conf.Port)
	}
}

func RunRPCServer(conf *config.Config) {
	manager = protocol.NewManager(conf)
	manager.ListenRPC()
}

func RunTerminal(conf *config.Config) {
	name := "gotty-linux"
	if runtime.GOOS == "darwin" {
		name = "gotty-darwin"
	}
	cmd := exec.Command(filepath.Join(conf.StaticDir, name), "--port", "4205", "-w",
		"bash", "-c", "echo 'Please input user name:'; read zrpcname; su $zrpcname")
	err := cmd.Start()
	if err != nil {
		log.Fatal("Error: start gotty failed", err)
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: zrpcd config.json service.json")
		return
	}
	conf := config.LoadConfig(os.Args[1])
	service.LoadService(os.Args[2])
	RunTerminal(conf)
	RunRPCServer(conf)
	RunHTTPServer(conf)
}
