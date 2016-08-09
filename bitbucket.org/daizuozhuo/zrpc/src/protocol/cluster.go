package protocol

import (
	"log"
	"time"

	"bitbucket.org/daizuozhuo/zrpc/src/config"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

func initCluster(conf *config.Config) {
	if conf.IP == conf.MasterIP {
		electState(conf)
	} else {
		workerState(conf)
	}
}

func electState(conf *config.Config) {
	key := conf.Cluster + "/master"
	for {
		kAPI := client.NewKeysAPI(conf.Etcd)
		_, err := kAPI.Set(context.Background(), key, conf.IP,
			&client.SetOptions{PrevExist: client.PrevNoExist, TTL: time.Second * 30})
		if err == nil {
			masterState(conf)
			return
		} else {
			log.Println("Warning: elect master", conf.IP, err.Error())
			if e, ok := err.(client.Error); ok &&
				e.Code == client.ErrorCodeNodeExist {
				//master already selected
				workerState(conf)
				return
			}
		}
		time.Sleep(time.Second)
	}
}

func masterState(conf *config.Config) {
	//you are master, maintain the master value
	key := conf.Cluster + "/master"
	conf.MasterIP = conf.IP
	conf.Master = conf.MasterIP + ":" + conf.Port
	for {
		kAPI := client.NewKeysAPI(conf.Etcd)
		_, err := kAPI.Set(context.Background(), key, conf.IP,
			&client.SetOptions{PrevValue: conf.IP, TTL: time.Second * 30})
		if err != nil {
			log.Println("Warning: update master", conf.IP, err.Error())
			if e, ok := err.(client.Error); ok &&
				e.Code == client.ErrorCodeTestFailed {
				//the master is changed, switch worker state
				workerState(conf)
				return
			}
		}
		time.Sleep(time.Second * 3)
	}
}

func workerState(conf *config.Config) {
	key := conf.Cluster + "/master"
	for {
		//get the current master IP
		kAPI := client.NewKeysAPI(conf.Etcd)
		res, err := kAPI.Get(context.Background(), key, nil)
		if err == nil {
			conf.MasterIP = res.Node.Value
			conf.Master = conf.MasterIP + ":" + conf.Port
			if conf.IP == res.Node.Value {
				masterState(conf)
				return
			}
			break
		}
		log.Println("Warn: get master IP", err.Error())
		time.Sleep(time.Second * 3)
	}

	//you are not master, watch the master value
	kAPI := client.NewKeysAPI(conf.Etcd)
	watcher := kAPI.Watcher(key, nil)
	for {
		res, err := watcher.Next(context.Background())
		if err != nil {
			log.Println("Warn: watch workers", err.Error())
			time.Sleep(time.Second * 3)
			continue
		}
		if res.Action == "expire" {
			//master expired, switch state
			electState(conf)
			return
		}
	}
}
