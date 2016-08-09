package protocol

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"bitbucket.org/daizuozhuo/zrpc/src/config"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type Manager struct {
	UID int

	current   *Master
	jobQueues []JobQueue
	members   map[string]*Member
}

// Member is a client machine
type Member struct {
	sync.RWMutex
	InGroup bool
	IP      string
	Name    string
	used    int
	NCPU    int
	GPU     bool
	Gluster bool
	LogURL  string
}

// WorkUnit is an cpu of member
type WorkUnit struct {
	member *Member
}

type gobServerCodec struct {
	rwc    io.ReadWriteCloser
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
	closed bool
}

func TimeoutCoder(f func(interface{}) error, e interface{}, msg string) error {
	echan := make(chan error, 1)
	go func() { echan <- f(e) }()
	select {
	case e := <-echan:
		return e
	case <-time.After(time.Minute):
		return fmt.Errorf("Timeout %s", msg)
	}
}
func (c *gobServerCodec) ReadRequestHeader(r *rpc.Request) error {
	return TimeoutCoder(c.dec.Decode, r, "server read request header")
}

func (c *gobServerCodec) ReadRequestBody(body interface{}) error {
	return TimeoutCoder(c.dec.Decode, body, "server read request body")
}

func (c *gobServerCodec) WriteResponse(r *rpc.Response, body interface{}) (err error) {
	if err = TimeoutCoder(c.enc.Encode, r, "server write response"); err != nil {
		if c.encBuf.Flush() == nil {
			log.Println("rpc: gob error encoding response:", err)
			c.Close()
		}
		return
	}
	if err = TimeoutCoder(c.enc.Encode, body, "server write response body"); err != nil {
		if c.encBuf.Flush() == nil {
			log.Println("rpc: gob error encoding body:", err)
			c.Close()
		}
		return
	}
	return c.encBuf.Flush()
}

func (c *gobServerCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}

// newManager makes manager according to config
func NewManager(config *config.Config) *Manager {
	manager := new(Manager)
	manager.UID = 0
	manager.members = make(map[string]*Member)
	manager.jobQueues = make([]JobQueue, 2)
	return manager
}

func (m *Manager) ListenRPC() {
	rpc.Register(NewWorker())
	l, e := net.Listen("tcp", ":4200")
	if e != nil {
		log.Fatal("Error: listen 4200 error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Print("Error: accept rpc connection", err.Error())
				continue
			}
			go func(conn net.Conn) {
				buf := bufio.NewWriter(conn)
				srv := &gobServerCodec{
					rwc:    conn,
					dec:    gob.NewDecoder(conn),
					enc:    gob.NewEncoder(buf),
					encBuf: buf,
				}
				err = rpc.ServeRequest(srv)
				if err != nil {
					log.Print("Error: server rpc request", err.Error())
				}
				srv.Close()
			}(conn)
		}
	}()
	//go initCluster(config.GetConfig())
	conf := config.GetConfig()
	if conf.IP == conf.MasterIP {
		go m.watchWorkers()
	}
	go m.processQueue()
}

func (m *Manager) NumMember() int {
	count := 0
	for _, member := range m.members {
		if member.InGroup {
			count += 1
		}
	}
	return count
}

func (m *Manager) AddJob(job Job, priority string) *Master {
	if job.GetID() == "" {
		job.SetID(m.MakeID())
	}
	master := NewMaster(m, job, job.GetID())
	master.Save()
	if priority == "Pro" {
		heap.Push(&m.jobQueues[0], master)
	} else {
		heap.Push(&m.jobQueues[1], master)
	}
	return master
}

func (m *Manager) processQueue() {
	for {
		for i, _ := range m.jobQueues {
			if len(m.jobQueues[i]) > 0 {
				j := heap.Pop(&m.jobQueues[i]).(*Master)
				m.current = j
				j.Run()
			}
		}
		m.current = nil
		time.Sleep(time.Millisecond * 10)
	}
}

func (m *Manager) SetMemberGluster(member *Member, flag bool) {
	/*
		if member.Gluster == true && flag == false {
			config.SendMail(fmt.Sprintf("%s Gluster Failed", member.Name))
		}
	*/
	member.Gluster = flag
}

func (m *Manager) TotalCPU() int {
	count := 0
	for _, member := range m.members {
		count += member.NCPU
	}
	return count
}

func (m *Manager) NumCPU() int {
	count := 0
	for _, member := range m.members {
		count += member.NCPU - member.used
	}
	return count
}

func (m *Manager) NumGPU() int {
	count := 0
	for _, member := range m.members {
		if member.GPU {
			count += 1
		}
	}
	return count
}

func (m *Manager) resetWorker(member *Member) bool {
	for i := 0; i < 3; i++ {
		err := safeCall(member, "Worker.ClearJob", struct{}{}, &Reply{})
		if err == nil {
			return true
		}
		log.Print("Error: ClearJob on ", member.IP, err.Error())
	}
	return false
}

func (m *Manager) addWorker(info *WorkerInfo) {
	member := &Member{
		InGroup: true,
		IP:      info.IP,
		Name:    info.Name,
		GPU:     info.GPU,
		Gluster: true,
		NCPU:    info.CPU,
		used:    0,
	}
	if m.resetWorker(member) {
		member.Lock()
		member.used = 0
		member.Unlock()
		m.members[member.Name] = member
	}
}

func (m *Manager) updateWorker(info *WorkerInfo) {
	member := m.members[info.Name]
	if member.InGroup == false {
		if m.resetWorker(member) {
			member.Lock()
			member.used = 0
			member.Unlock()
		}
	}
	member.NCPU = info.CPU
	member.InGroup = true
	member.GPU = info.GPU
	member.IP = info.IP
}

func (m *Manager) nodeToWorkerInfo(node *client.Node) *WorkerInfo {
	info := &WorkerInfo{}
	err := json.Unmarshal([]byte(node.Value), info)
	if err != nil {
		log.Print(err)
	}
	return info
}

func (m *Manager) watchWorkers() {
	config := config.GetConfig()
	url := fmt.Sprintf("%s/workers", config.Cluster)
	kAPI := client.NewKeysAPI(config.Etcd)
	watcher := kAPI.Watcher(url, &client.WatcherOptions{
		Recursive: true,
	})
	for {
		res, err := watcher.Next(context.Background())
		if err != nil {
			log.Println("Warn: watch workers", err.Error())
			time.Sleep(time.Second * 3)
			continue
		}
		if res.Action == "expire" {
			info := m.nodeToWorkerInfo(res.PrevNode)
			member, ok := m.members[info.Name]
			log.Println("expire", info.Name)
			if ok {
				member.InGroup = false
			}
		} else if res.Action == "set" {
			info := m.nodeToWorkerInfo(res.Node)
			if _, ok := m.members[info.Name]; ok {
				m.updateWorker(info)
			} else {
				m.addWorker(info)
			}
		}
	}

}

func (m *Manager) MakeID() string {
	m.UID += 1
	return fmt.Sprintf("%s%d", time.Now().Format("2006/01/02 15:04:05 "), m.UID)
}

func (m *Manager) ExecuteAll(funcName string, args interface{}) (string, error) {
	var buffer bytes.Buffer
	done := make(chan bool, len(m.members))
	for _, member := range m.members {
		go func(mem *Member) {
			err := call(mem.IP, funcName, args, &Reply{})
			if err != nil {
				buffer.WriteString(fmt.Sprintf("Error: execute %s on %s faild", funcName, mem.Name))
			}
			done <- true
		}(member)
	}
	for i := 0; i < len(m.members); i++ {
		<-done
	}
	return buffer.String(), nil
}

type MemberInfo struct {
	Info string
	*Member
}

type ClusterStatus struct {
	QueueInfo   []string
	MemberInfos []*MemberInfo
}

func (m *Manager) Status() *ClusterStatus {
	reply := &ClusterStatus{}
	reply.MemberInfos = make([]*MemberInfo, len(m.members))
	for _, q := range m.jobQueues {
		for _, j := range q {
			info := fmt.Sprintf("%s: %v %v", j.GetUser(), j.GetArgs(), j.GetStatus())
			reply.QueueInfo = append(reply.QueueInfo, info)
		}
	}

	var keys []string
	for k := range m.members {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		member := m.members[k]
		var status string
		if member.InGroup {
			status = "Live"
		} else {
			status = "Dead"
		}
		if !member.GPU {
			status += " GPU is not supported"
		}
		if !member.Gluster {
			status += " Gluster error"
		}
		reply.MemberInfos[i] = &MemberInfo{
			fmt.Sprintf("%s : used/total %d/%d %s", k, member.used, member.NCPU, status),
			member,
		}
	}
	return reply
}

func (m *Manager) RequestPreferMember(c *Constraint, name string) (*Member, error) {
	n := c.Threads
	member := m.members[name]
	for {
		find := false
		if (!member.InGroup) ||
			(c.GPU && !member.GPU) ||
			(c.ExcludeIP == member.IP) {
			return nil, fmt.Errorf("Prefer Member is unavailable")
		}
		member.Lock()
		free := member.NCPU - member.used
		if n == -1 {
			if member.used == 0 {
				member.used = member.NCPU
				find = true
			}
		} else if free >= n {
			member.used += n
			find = true
		}
		member.Unlock()
		if find {
			return member, nil
		} else {
			time.Sleep(time.Microsecond * 100)
		}
	}
}

// RequestMember request idle member from master
func (m *Manager) RequestMember(c *Constraint) (*Member, error) {
	n := c.Threads
	var keys []string
	count := 0
	var index int = -1
	for k, v := range m.members {
		if c.PreferIP != "" && c.PreferIP == v.IP {
			index = count
		}
		keys = append(keys, k)
		count++
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("Member list is empty")
	}
	if index != -1 {
		member, err := m.RequestPreferMember(c, keys[index])
		if err == nil {
			return member, err
		}
	}
	for index = rand.Int() % len(keys); true; index++ {
		if index == len(keys) {
			index = 0
			time.Sleep(time.Microsecond * 100)
		}
		member := m.members[keys[index]]
		find := false
		if (!member.InGroup) ||
			(c.GPU && !member.GPU) ||
			(c.ExcludeIP == member.IP) {
			continue
		}
		member.Lock()
		free := member.NCPU - member.used
		if n == -1 {
			if member.used == 0 {
				member.used = member.NCPU
				find = true
			}
		} else if free >= n {
			member.used += n
			find = true
		}
		member.Unlock()
		if find {
			return member, nil
		}
	}
	return nil, fmt.Errorf("cannnot request member")
}

// ReleaseMember release member to master
func (m *Manager) ReleaseMember(member *Member, c *Constraint) error {
	member.Lock()
	if c.Threads == -1 {
		member.used = 0
	} else {
		member.used -= c.Threads
		if member.used < 0 {
			member.used = 0
		}
	}
	member.Unlock()
	return nil
}
