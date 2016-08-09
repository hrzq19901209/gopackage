package service

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-fsnotify/fsnotify"
)

type ArgumentType int

const (
	DEFAULT ArgumentType = iota
	OPTION
	INPUTFILE
	INPUTFOLDER
	INPUTPREFIX
	OUTPUTFILE
	OUTPUTFOLDER
	OUTPUTPREFIX
)

var TypeMap = map[string]ArgumentType{
	"":             DEFAULT,
	"DEFAULT":      DEFAULT,
	"OPTION":       OPTION,
	"INPUTFILE":    INPUTFILE,
	"INPUTFOLDER":  INPUTFOLDER,
	"INPUTPREFIX":  INPUTPREFIX,
	"OUTPUTFILE":   OUTPUTFILE,
	"OUTPUTFOLDER": OUTPUTFOLDER,
	"OUTPUTPREFIX": OUTPUTPREFIX,
}

type ServiceHub map[string]*Service

var serviceHub *ServiceHub

type Service struct {
	Name     string
	Args     []string
	ArgsType []string

	Types []ArgumentType
}

func (s *Service) Bind(data map[string]string) ([]string, error) {
	results := make([]string, len(s.Args))
	for i, a := range s.Args {
		if a[0] == '$' {
			r, ok := data[a[1:]]
			if !ok {
				return results, fmt.Errorf("cannnot find argument %s", a)
			}
			results[i] = r
		} else {
			results[i] = a
		}
	}
	return results, nil
}

func GetServices() *ServiceHub {
	if serviceHub == nil {
		log.Fatal("Error: services not loaded")
	}
	return serviceHub
}

func validateService(fileName string) (*ServiceHub, error) {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		return nil, fmt.Errorf("Error: open service file %s %s\n", fileName, err.Error())
	}
	var hub ServiceHub
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&hub)
	if err != nil {
		return nil, fmt.Errorf("Error: decode service config error %s\n", err.Error())
	}
	for _, s := range hub {
		if len(s.Args) != len(s.ArgsType) {
			fmt.Errorf("service %s Args and ArgsType's are not equal", s.Name)
		}
		s.Types = make([]ArgumentType, len(s.Args))
		for i, t := range s.ArgsType {
			if t == "" {
				s.ArgsType[i] = "DEFAULT"
			}
			argumentType, ok := TypeMap[t]
			if !ok {
				return nil, fmt.Errorf("Set Service fail, cannot find type %s", t)
			}
			s.Types[i] = argumentType
		}
	}
	return &hub, nil
}
func LoadService(fileName string) *ServiceHub {
	hub, err := validateService(fileName)
	if err != nil {
		log.Fatal(err.Error())
	}
	serviceHub = hub
	go watchFile(fileName)
	return serviceHub
}

func watchFile(filename string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Println("modifed file:", event.Name)
				}
				hub, err := validateService(filename)
				if err == nil {
					serviceHub = hub
				}
			case err := <-watcher.Errors:
			 	if err != nil {
					log.Println("error:", err)
				}
			}
			time.Sleep(time.Second)
		}
	}()
	err = watcher.Add(filename)
	if err != nil {
		log.Fatal(err)
	}
}
