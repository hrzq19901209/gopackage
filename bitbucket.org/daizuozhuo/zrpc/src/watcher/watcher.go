package main

import (
	"gopkg.in/fsnotify.v1"
	"log"
	"os"
	"os/exec"
	"syscall"
	"zsystem"
)

var cmd *exec.Cmd

func onChange(src string) {
	zsystem.SendCtrlBreak(cmd.Process.Pid)
	run(src)
}

func watch(filename string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				log.Println("event", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Println("modifed file:", event.Name)
				}
				onChange(filename)
				done <- true
			case err := <-watcher.Errors:
				log.Println("error:", err)
				done <- true
			}
		}
	}()
	err = watcher.Add(filename)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func run(src string) {
	dst := `C:\temp`
	zsystem.Copy(src, dst)

	cmd = exec.Command("cmd.exe", "/C", dst)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
	log.Printf("start running %s\n", src)
	go cmd.Start()
	watch(src)
}

func main() {
	args := os.Args
	run(args[1])
}
