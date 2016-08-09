package protocol

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"strings"
	"time"
	"unicode"

	"bitbucket.org/daizuozhuo/zrpc/src/config"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type Status int

const (
	Waiting Status = iota
	Running
	Success
	Failure
	Killed
	Preserve
)

var StatusMap = map[Status]string{
	Waiting: "Waiting",
	Running: "Running",
	Success: "Success",
	Failure: "Failure",
	Killed:  "Killed",
}

func (s Status) String() string {
	return StatusMap[s]
}

type Reply struct {
	Ok  bool
	Msg string
}

func MoveDir(source string, dest string) (err error) {
	if !strings.HasSuffix(source, "/") {
		source += "/"
	}
	if !strings.HasSuffix(dest, "/") {
		source += "/"
	}
	args := fmt.Sprintf("mv %s* %s", source, dest)
	cmd := exec.Command("bash", "-c", args)
	return cmd.Run()
}

type gobClientCodec struct {
	rwc    io.ReadWriteCloser
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
}

func (c *gobClientCodec) WriteRequest(r *rpc.Request, body interface{}) (err error) {
	if err = TimeoutCoder(c.enc.Encode, r, "client write request"); err != nil {
		return
	}
	if err = TimeoutCoder(c.enc.Encode, body, "client write request body"); err != nil {
		return
	}
	return c.encBuf.Flush()
}

func (c *gobClientCodec) ReadResponseHeader(r *rpc.Response) error {
	return c.dec.Decode(r)
}

func (c *gobClientCodec) ReadResponseBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *gobClientCodec) Close() error {
	return c.rwc.Close()
}

func safeCall(m *Member, rpcname string, args interface{}, reply interface{}) error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- call(m.IP, rpcname, args, reply)
	}()
	for {
		select {
		case err := <-errChan:
			return err
		case <-time.After(time.Minute):
			if m.InGroup == false {
				return fmt.Errorf("ConnectError: %s dead", m.Name)
			}
		}
	}
	return nil
}

func call(srv string, rpcname string, args interface{}, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", srv+":4200", time.Second*10)
	if err != nil {
		return fmt.Errorf("DialError: %s", err.Error())
	}
	encBuf := bufio.NewWriter(conn)
	codec := &gobClientCodec{conn, gob.NewDecoder(conn), gob.NewEncoder(encBuf), encBuf}
	c := rpc.NewClientWithCodec(codec)
	err = c.Call(rpcname, args, reply)
	errc := c.Close()
	if errc != nil {
		log.Print("Error: Close rpc client", errc)
	}
	return err
}

func saveToEtcd(key string, value string) {
	conf := config.GetConfig()
	kAPI := client.NewKeysAPI(conf.Etcd)
	key = conf.Cluster + "/" + key
	_, err := kAPI.Set(context.Background(), key, value, nil)
	if err != nil {
		log.Printf("Warning: save %s %s fail", key, value)
	}
}

func normalizeID(id string) string {
	var ID string
	for _, c := range id {
		if unicode.IsDigit(c) || unicode.IsLetter(c) {
			ID += string(c)
		}
	}
	return ID
}
