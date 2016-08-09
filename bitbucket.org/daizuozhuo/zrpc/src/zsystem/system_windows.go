// +build windows

package zsystem

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func SendCtrlBreak(pid int) {
	d, e := syscall.LoadDLL("kernel32.dll")
	if e != nil {
		log.Fatalf("LoadDLL: %v\n", e)
	}
	p, e := d.FindProc("GenerateConsoleCtrlEvent")
	if e != nil {
		log.Fatalf("FindProc: %v\n", e)
	}
	r, _, e := p.Call(syscall.CTRL_BREAK_EVENT, uintptr(pid))
	if r == 0 {
		log.Fatalf("GenerateConsoleCtrlEvent: %v\n", e)
	}
}

func Typeperf() []string {
	//cmd /C typeperf "\processor(_total)\% processor time" "\Memory\% Committed Bytes In Use"
	cmd := exec.Command("cmd.exe", "/C", "typeperf",
		`\processor(_total)\% processor time`,
		`\Memory\% Committed Bytes In Use`,
		`\LogicalDisk(D:)\% Free Space`,
		`\System\System Up Time`, "-sc", "1")

	out, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("typerf out pipe error", err)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
	cmd.Start()

	line := make([]byte, 1024)
	var n int
	//cpu, mem, disk, up time
	results := make([]string, 4)

	for i := 0; i < 5; {
		n, err = out.Read(line)
		if err == nil && n != 0 {
			msg := string(line[:n])
			if len(strings.Split(msg, "/")) > 1 {
				i = 1
			} else if i > 0 {
				results[i-1] = msg[2 : len(msg)-1]
				i++
			}
		}
	}
	// SendCtrlBreak(cmd.Process.Pid)
	fmt.Println(results)
	return results
}

func nativeStatus() *SystemStatus {
	status := new(SystemStatus)

	typepref_res := Typeperf()

	status.CPUUsage, _ = strconv.ParseFloat(typepref_res[0], 64)
	status.MemoryUsage, _ = strconv.ParseFloat(typepref_res[1], 64)
	status.HarddiskUsage, _ = strconv.ParseFloat(typepref_res[2], 64)
	status.Uptime, _ = strconv.ParseFloat(typepref_res[3], 64)

	return status
}

func copy(src string, dst string) {
	cmd := exec.Command("cmd.exe", "/C", "xcopy", "/Y", src, dst)
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Copy %s to %s: %v\n", src, dst, err)
	}
}

func move(src string, dst string) {
	cmd := exec.Command("cmd.exe", "/C", "move", "/Y", src, dst)
	err := cmd.Run()
	if err != nil {
		log.Fatalf("move %s to %s: %v\n", src, dst, err)
	}
}
