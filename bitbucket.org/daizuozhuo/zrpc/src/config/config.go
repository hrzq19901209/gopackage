package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/smtp"
	"os"
	"time"

	"github.com/coreos/etcd/client"
)

var config *Config

type configInfo struct {
	Name          string
	IP            string
	MasterIP      string
	Port          string
	Cluster       string
	StaticDir     string
	LogDir        string
	EtcdEndpoints []string
	Timeout       int64 `json:",string"`

	Email    string
	Passwd   string
	MailHost string
}

type Config struct {
	*configInfo
	Master       string
	Auth         smtp.Auth
	LogPath      string
	LogFile      *os.File
	Etcd	client.Client
	WorkerLogger *Logger
	MasterLogger *Logger
}

func GetConfig() *Config {
	if config == nil {
		log.Fatalf("Error: config file not loaded")
	}
	return config
}

func LoadConfig(fileName string) *Config {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Error: open config file %s %v\n", fileName, err)
	}
	info := readconfigInfo(file)

	cfg := client.Config{
		Endpoints:               info.EtcdEndpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	etcdClient, err := client.New(cfg)
	if err != nil {
		log.Fatal("Error: cannot connec to etcd:", err)
	}
	var auth smtp.Auth
	if info.Email != "" {
		auth = smtp.PlainAuth(info.Cluster, info.Email, info.Passwd, info.MailHost)
	}
	config = &Config{
		configInfo:   info,
		Auth:         auth,
		Master:       info.MasterIP + ":" + info.Port,
		WorkerLogger: NewLogger(info.LogDir, info.Name),
		MasterLogger: NewLogger(info.StaticDir, "master"),
		Etcd: etcdClient,
	}
	log.SetOutput(config.MasterLogger.LogFile)
	go config.WorkerLogger.Serve(info.StaticDir, 4207)
	go config.WorkerLogger.RotateLog()
	go config.MasterLogger.Serve(info.StaticDir, 4208)
	return config
}

func SendMail(message string) {
	if config.Auth != nil {
		receiver := "daizuozhuo@gmail.com"
		from := config.Email
		to := []string{receiver}
		msg := []byte(fmt.Sprintf("To: %s\r\nSubject: zrpc\r\n\r\n%s %s",
			receiver, config.Cluster, message))
		err := smtp.SendMail(config.MailHost+":587", config.Auth, from, to, msg)
		if err != nil {
			log.Print("Error: send mail ", err)
		}
	}
}

func readconfigInfo(reader io.Reader) *configInfo {
	configInfo := &configInfo{}
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(configInfo)
	if err != nil {
		log.Fatalf("Error: decode config from reader %v\n", err)
	}
	return configInfo
}
