package config

import (
	"fmt"
	stdLog "log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

type Logger struct {
	*stdLog.Logger
	LogDir      string
	Prefix      string
	LogPath     string
	LogFile     *os.File
	LogPathChan chan string
}

func NewLogger(dir string, prefix string) *Logger {
	logName := fmt.Sprintf("%s-%s.log", prefix, time.Now().Format("01-02:15:04:05"))
	logPath := filepath.Join(dir, logName)
	logFile, err := os.Create(logPath)
	if err != nil {
		stdLog.Print("Error: open file", err)
		return nil
	}
	l := &Logger{
		Logger:      stdLog.New(logFile, "", stdLog.LstdFlags),
		LogDir:      dir,
		Prefix:      prefix,
		LogPath:     logPath,
		LogFile:     logFile,
		LogPathChan: make(chan string, 1),
	}
	l.LogPathChan <- logPath
	return l
}

func (l *Logger) Serve(ttyDir string, port int) {
	name := "gotty-linux"
	if runtime.GOOS == "darwin" {
		name = "gotty-darwin"
	}
	var cmd *exec.Cmd
	for {
		newPath := <-l.LogPathChan
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
		}
		cmd = exec.Command(filepath.Join(ttyDir, name), "--port", strconv.Itoa(port),
			"tail", "-f", "-c", "2m", newPath)
		err := cmd.Start()
		if err != nil {
			stdLog.Fatal("Error: start log server failed", err)
		}
	}
}

func (l *Logger) RotateLog() {
	for {
		fileInfo, err := os.Stat(l.LogPath)
		if err != nil {
			stdLog.Print("Get log file stat error", err)
			return
		}
		if fileInfo.Size() > 1024*1024*200 { //200 MB
			logName := fmt.Sprintf("%s-%s.log", l.Prefix, time.Now().Format("01-02:15:04:05"))
			logPath := filepath.Join(l.LogDir, logName)
			logFile, err := os.Create(logPath)
			if err != nil {
				stdLog.Print("Error: open file", err)
				return
			}
			l.LogFile.Close()
			l.Logger.SetOutput(logFile)
			l.LogPathChan <- logPath
			l.LogFile = logFile
			l.LogPath = logPath
		}
		time.Sleep(time.Minute * 1)
	}
}
