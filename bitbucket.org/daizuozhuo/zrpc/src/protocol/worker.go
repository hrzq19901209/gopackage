package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"bitbucket.org/daizuozhuo/zrpc/src/config"
	"bitbucket.org/daizuozhuo/zrpc/src/user"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

var wlog *config.Logger

type TaskInfo struct {
	*exec.Cmd
	Status
	ErrChan chan error
}

type Worker struct {
	sync.RWMutex
	Tasks map[string]*TaskInfo
}

func NewWorker() *Worker {
	config := config.GetConfig()
	wlog = config.WorkerLogger
	w := &Worker{
		Tasks: make(map[string]*TaskInfo),
	}
	go HeartBeat(config)
	return w
}

func (w *Worker) deleteTask(key string) {
	w.Lock()
	delete(w.Tasks, key)
	w.Unlock()
}

func (w *Worker) getTask(key string) (*TaskInfo, bool) {
	w.RLock()
	result, ok := w.Tasks[key]
	w.RUnlock()
	return result, ok
}

func (w *Worker) setTask(key string, task *TaskInfo) {
	w.Lock()
	w.Tasks[key] = task
	w.Unlock()
}

// workerInfo is the service register information to etcd
type WorkerInfo struct {
	Name string
	IP   string
	GPU  bool
	CPU  int
}

func HeartBeat(config *config.Config) {
	api := client.NewKeysAPI(config.Etcd)

	for {
		//check if GPU is available
		cmd := exec.Command("bash", "-c", "env DISPLAY=:0 glxinfo | grep NVIDIA")
		_, err := cmd.CombinedOutput()
		info := &WorkerInfo{
			Name: config.Name,
			IP:   config.IP,
			GPU:  err == nil,
			CPU:  runtime.NumCPU(),
		}

		key := fmt.Sprintf("%s/workers/%s", config.Cluster, config.Name)
		value, _ := json.Marshal(info)

		_, err = api.Set(context.Background(), key, string(value), &client.SetOptions{
			TTL: time.Minute * 30,
		})
		if err != nil {
			wlog.Println("Warning: update workerInfo", err)
		}
		time.Sleep(time.Second * 3)
	}
}

func (w *Worker) GetLogURL(_, reply *Reply) error {
	reply.Msg = fmt.Sprintf("public/%s", config.GetConfig().LogPath)
	return nil
}

func PostUpdate(ID string, progress float64) {
	config := config.GetConfig()
	url := fmt.Sprintf("http://%s/cmd/update", config.Master)
	update := JobUpdate{ID, progress}
	buf, err := json.Marshal(update)
	if err != nil {
		wlog.Print("Warn: Marshal JobUpdate struct", err.Error())
	}
	res, err := http.Post(url, "application/json", bytes.NewBuffer(buf))
	if err != nil {
		wlog.Print("Warn: post JobUpdate ", err.Error())
	}
	text, _ := ioutil.ReadAll(res.Body)
	wlog.Print(string(text))
}

func MonitorLog(reader io.Reader, ID string) {
	defer func() {
		if r := recover(); r != nil {
			wlog.Print("Recovered in writeToLog")
		}
	}()
	prefix := "##########"
	scanner := bufio.NewReader(reader)
	for {
		s, err := scanner.ReadString('\n')
		if len(s) > 0 && s[0] != '\n' {
			wlog.Print(s)
		}
		if strings.HasPrefix(s, prefix) {
			s = strings.Trim(s, "# \n")
			progress, err := strconv.ParseFloat(s, 64)
			if err != nil {
				wlog.Print("Warn: passing progress float", s, err.Error())
				continue
			}
			PostUpdate(ID, progress)
		}
		if err != nil {
			if err != io.EOF {
				wlog.Print("Warn: scanner error", err.Error())
			}
			break
		}
	}
	PostUpdate(ID, 100)
}

func WriteToLog(reader io.Reader) {
	defer func() {
		if r := recover(); r != nil {
			wlog.Print("Recovered in writeToLog")
		}
	}()
	scanner := bufio.NewReader(reader)
	for {
		s, err := scanner.ReadString('\n')
		if len(s) > 0 && s[0] != '\n' {
			wlog.Print(s)
		}
		if err != nil {
			if err != io.EOF {
				wlog.Print("Warn: scanner error", err.Error())
			}
			break
		}
	}
}

func (w *Worker) check(id string) error {
	task, ok := w.getTask(id)
	if ok {
		if task.Status == Killed { //the task was killed
			return fmt.Errorf("Killed")
		}
		if task != nil && task.Process != nil { //the job is running
			pgid, err := syscall.Getpgid(task.Process.Pid)
			if err == nil {
				syscall.Kill(-pgid, 15)
			}
		}
	}
	config := config.GetConfig()
	cmd := exec.Command("ls", filepath.Join(config.LogDir, "conf_release.json"))
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("GlusterError: %s", err.Error())
	}
	return nil
}

func (w *Worker) DoServiceJob(task *ServiceTask, reply *ServiceReply) error {
	info := &TaskInfo{
		Status:  Running,
		ErrChan: make(chan error, 1),
	}
	if err := w.check(task.ID); err != nil {
		info.ErrChan <- err
		return err
	}
	args := task.Args
	wlog.Println("Worker: do service task", args)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(os.Environ(), "ZRPCPID="+task.ID)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	info.Cmd = cmd
	if task.ID != "" {
		w.setTask(task.ID, info)
	}
	reader, err := cmd.StdoutPipe()
	if err != nil {
		reply.Msg = fmt.Sprintf("RPCError: create stdoutpipe from cmd %v", err)
		err := errors.New(reply.Msg)
		info.ErrChan <- err
		return err
	}
	go MonitorLog(reader, task.ID)
	errReader, err := cmd.StderrPipe()
	if err != nil {
		reply.Msg = fmt.Sprintf("RPCError: create stderrpipe from cmd %v", err)
		err := errors.New(reply.Msg)
		info.ErrChan <- err
		return err
	}
	go WriteToLog(errReader)
	err = cmd.Start()
	if err == nil {
		err = cmd.Wait()
	}
	if info.Status == Killed {
		reply.Msg = "Info: Killed"
		reply.Ok = true
		err = fmt.Errorf("Killed")
	} else if err != nil {
		reply.Msg = fmt.Sprintf("RPCError: worker do job %v %s", args, err.Error())
		reply.Ok = false
		err = fmt.Errorf("%s", reply.Msg)
	} else {
		reply.Msg = "Info: OK"
		reply.Ok = true
	}
	info.ErrChan <- err
	return err
}

func (w *Worker) DoJob(task *BasicTask, reply *Reply) error {
	info := &TaskInfo{
		Status:  Running,
		ErrChan: make(chan error, 1),
	}
	if err := w.check(task.ID); err != nil {
		info.ErrChan <- err
		return err
	}
	args := task.Args
	wlog.Println("Worker: do job", args)
	cmd := exec.Command(args[0], args[1:]...)
	info.Cmd = cmd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if task.ID != "" {
		w.setTask(task.ID, info)
	}
	reader, err := cmd.StdoutPipe()
	if err != nil {
		reply.Msg = fmt.Sprintf("RPCError: create stdoutpipe from cmd %v", err)
		err := errors.New(reply.Msg)
		info.ErrChan <- err
		return err
	}
	go WriteToLog(reader)
	errReader, err := cmd.StderrPipe()
	if err != nil {
		reply.Msg = fmt.Sprintf("RPCError: create stderrpipe from cmd %v", err)
		err := errors.New(reply.Msg)
		info.ErrChan <- err
		return err
	}
	go WriteToLog(errReader)
	err = cmd.Start()
	if err == nil {
		err = cmd.Wait()
	}
	if info.Status == Killed {
		reply.Msg = "Killed"
		reply.Ok = true
		err = errors.New(reply.Msg)
	} else if err != nil {
		reply.Msg = fmt.Sprintf("RPCError: worker do job %v %v", args, err)
		reply.Ok = false
		err = errors.New(reply.Msg)
	} else {
		reply.Msg = "Info: OK"
		reply.Ok = true
	}
	info.ErrChan <- err
	return err
}

func (w *Worker) RecoverJob(task *BasicTask, reply *Reply) error {
	info, ok := w.getTask(task.ID)
	if !ok {
		return errors.New("RPCError: no such task")
	}
	err := <-info.ErrChan
	info.ErrChan <- err
	return err
}

func (w *Worker) DoShadowJob(task *ClonableTask, reply *Reply) error {
	info := &TaskInfo{
		Status:  Running,
		ErrChan: make(chan error, 1),
	}
	temp, err := ioutil.TempDir("", "shadow")
	if err != nil {
		return fmt.Errorf("RPCError: cannot create temp dir %s", err.Error())
	}
	task.Args[task.Output] = temp
	wlog.Println("Worker: do shadow job", task.Args)
	cmd := exec.Command(task.Args[0], task.Args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	info.Cmd = cmd
	w.setTask(task.ID, info)
	reader, err := cmd.StdoutPipe()
	if err != nil {
		wlog.Print("Error: create stdoutpipe from cmd", err)
	}
	go WriteToLog(reader)
	errReader, _ := cmd.StderrPipe()
	go WriteToLog(errReader)
	err = cmd.Start()
	if err == nil {
		err = cmd.Wait()
	}
	if info.Status == Killed {
		reply.Msg = "Info: Killed"
		reply.Ok = true
		err = fmt.Errorf("Killed")
	} else if err != nil {
		reply.Msg = fmt.Sprintf("RPCError: worker do job %v %s", task.Args, err.Error())
		reply.Ok = false
		err = fmt.Errorf("%s", reply.Msg)
	} else {
		reply.Msg = "Info: OK"
		reply.Ok = true
	}
	return err
}

func (w *Worker) CollectShadowResult(task *ClonableTask, reply *Reply) error {
	if err := w.check(task.ID); err != nil {
		return err
	}
	wlog.Println("Worker: collect shadow result", task.ID)
	shadow, ok := w.getTask(task.ID)
	if !ok {
		return fmt.Errorf("shadow task not found")
	}
	wlog.Println("Worker: move %s to %s", shadow.Args[task.Output], task.Args[task.Output])
	return MoveDir(shadow.Args[task.Output], task.Args[task.Output])
}

func (w *Worker) StopJob(id string, reply *Reply) error {
	wlog.Println("Worker: stop job", id)
	job, ok := w.getTask(id)
	if !ok {
		taskInfo := &TaskInfo{Status: Killed}
		w.setTask(id, taskInfo)
		return nil
	}
	if job == nil {
		return nil
	}
	job.Status = Killed
	if job.Process != nil {
		pgid, err := syscall.Getpgid(job.Process.Pid)
		if err == nil {
			syscall.Kill(-pgid, 15)
			job.Process = nil
		}
	}
	reply.Ok = true
	return nil
}

func (w *Worker) DeleteJob(id string, reply *Reply) error {
	w.deleteTask(id)
	return nil
}

func (w *Worker) ClearJob(_, reply *Reply) error {
	wlog.Print("Worker: clean all jobs")
	w.Lock()
	defer w.Unlock()
	for _, job := range w.Tasks {
		if job != nil && job.Process != nil {
			pgid, err := syscall.Getpgid(job.Process.Pid)
			if err == nil {
				syscall.Kill(-pgid, 15)
				job.Process = nil
			}
		}
	}
	reply.Ok = true
	return nil
}

func (w *Worker) AddUsers(users []*user.LinuxUser, reply *Reply) error {
	var errMsg string
	for _, u := range users {
		err := user.AddUser(u)
		if err != nil {
			errMsg += err.Error()
		}
	}
	return errors.New(errMsg)
}
