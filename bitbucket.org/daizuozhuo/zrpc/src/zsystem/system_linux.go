// +build linux
package zsystem

import (
	"bitbucket.org/bertimus9/systemstat"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func getMemoryStatistic() *MemoryStatistic {
	var stat = new(MemoryStatistic)
	sample := systemstat.GetMemSample()
	stat.Free = sample.MemFree
	stat.Active = sample.MemUsed
	stat.Total = sample.MemTotal
	return stat
}

func getUptime() float64 {
	contents, err := ioutil.ReadFile("/proc/uptime")
	if err != nil {
		log.Fatal(err)
	}
	fields := strings.Fields(string(contents))
	upTime, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		fmt.Println(err)
		return -1
	}
	return upTime
}

func getCPUUsage() float64 {
	first := systemstat.GetCPUSample()
	time.Sleep(100 * time.Millisecond)
	second := systemstat.GetCPUSample()
	average := systemstat.GetSimpleCPUAverage(first, second)
	return average.BusyPct
}

func nativeStatus() *SystemStatus {
	status := new(SystemStatus)

	var stat syscall.Statfs_t

	syscall.Statfs("/", &stat)

	status.Name, _ = os.Hostname()
	status.HarddiskFreeSpace = stat.Bavail * uint64(stat.Bsize)
	status.HarddiskUsage = 100.0 - (float64(stat.Bavail*uint64(stat.Bsize)) * 100.0 / float64(stat.Blocks*uint64(stat.Bsize)))
	status.Uptime = getUptime()

	memstat := getMemoryStatistic()
	status.MemoryUsage = float64(memstat.Active) / float64(memstat.Total)
	status.CPUUsage = getCPUUsage()

	/*
	fmt.Println("Free space (B) : ", status.HarddiskFreeSpace)
	fmt.Println("Space Usage (%) : ", status.HarddiskUsage)
	fmt.Println("Uptime (s) : ", status.Uptime)
	fmt.Println("Memory Usage (%) : ", status.MemoryUsage)
	fmt.Println("CPU Usage (%) : ", status.CPUUsage)
	*/

	return status
}

func copy(src string, dst string) {
	cmd := exec.Command("cp", src, dst)
	if err := cmd.Run(); err != nil {
		log.Fatalf("Copy %s to %s: %v\n", src, dst, err)
	}
}
func move(src string, dst string) {
	cmd := exec.Command("mv", src, dst)
	if err := cmd.Run(); err != nil {
		log.Fatalf("move %s to %s: %v\n", src, dst, err)
	}
}
