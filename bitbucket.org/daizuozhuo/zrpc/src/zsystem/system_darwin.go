// +build darwin

package zsystem

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func getSysctrlString(key string) string {
	out, err := exec.Command("sysctl", key).Output()
	if err != nil {
		log.Fatal(err)
	}
	msg := string(out[:len(out)])
	msgs := strings.Split(msg, ":")

	if len(msgs) > 1 {
		res := strings.TrimSpace(msgs[1])
		return res
	} else {
		return "Unknown"
	}
}

// Just a practice, it is replaced by os.Hostname
func getHostname() string {
	return getSysctrlString("kern.hostname")
}

func getMemsize() uint64 {
	value, _ := strconv.ParseInt(getSysctrlString("hw.memsize"), 10, 64)
	return uint64(value)
}

func getMemoryStatistic() *MemoryStatistic {
	var stat = new(MemoryStatistic)
	out, err := exec.Command("vm_stat").Output()
	if err != nil {
		log.Fatal(err)
	}
	msg := string(out[:len(out)])
	lines := strings.Split(msg, "\n")
	num_lines := len(lines)
	for i := 1; i < num_lines; i++ {
		line := lines[i]
		info := strings.Split(line, ":")
		if len(info) == 2 {
			tag := strings.TrimSpace(info[0])
			value, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimRight(info[1], ".")), 10, 64)
			num := uint64(value)
			// fmt.Println(tag)
			// fmt.Println(num)
			if tag == "Pages free" {
				stat.Free = num * 4096
			} else if tag == "Pages inactive" {
				stat.Inactive = num * 4096
			} else if tag == "Pages active" {
				stat.Active = num * 4096
			} else if tag == "Pages wired down" {
				stat.Wired = num * 4096
			}
		}
	}

	return stat
}

func getUptime() float64 {
	out, err := exec.Command("date", "+%s").Output()
	if err != nil {
		log.Fatal(err)
		return -1
	}
	current_time, err := strconv.ParseFloat(strings.TrimSpace(string(out[:len(out)])), 64)
	if err != nil {
		fmt.Println(err)
		return -1
	}
	// fmt.Println(current_time)

	out, err = exec.Command("sysctl", "kern.boottime").Output()
	if err != nil {
		log.Fatal(err)
		return -1
	}
	msg := strings.TrimLeft(string(out[:len(out)]), "kern.boottime: { sec = ")
	msgs := strings.Split(msg, ",")

	if len(msgs) > 1 {
		// fmt.Println(msgs)
		boot_time, _ := strconv.ParseFloat(msgs[0], 64)
		// fmt.Println(boot_time)
		return current_time - boot_time
	} else {
		return -1
	}
}

func getCPUUsage() float64 {
	out, err := exec.Command("top", "-l", "1", "-R").Output()
	if err != nil {
		log.Fatal(err)
	}
	msg := string(out[:len(out)])
	lines := strings.Split(msg, "\n")
	num_lines := len(lines)
	for i := 1; i < num_lines; i++ {
		line := lines[i]
		info := strings.Split(line, ":")
		if len(info) == 2 {
			tag := strings.TrimSpace(info[0])
			if tag == "CPU usage" {
				values := strings.Split(info[1], ",")
				if len(values) == 3 {
					idle_values := strings.Split(values[2], "%")
					// fmt.Println(values[2])
					if len(idle_values) == 2 {
						value, err := strconv.ParseFloat(strings.TrimSpace(idle_values[0]), 64)
						if err != nil {
							fmt.Println(err)
							return 0
						}
						return 100.0 - value
					} else {
						return 0
					}
				} else {
					return 0
				}
			}
		}
	}

	return 0
}

func nativeStatus() *SystemStatus {
	status := new(SystemStatus)

	fmt.Println("mac status")
	var stat syscall.Statfs_t

	syscall.Statfs("/", &stat)

	status.Name, _ = os.Hostname()
	status.HarddiskFreeSpace = stat.Bavail * uint64(stat.Bsize)
	status.HarddiskUsage = 100.0 - (float64(stat.Bavail*uint64(stat.Bsize)) * 100.0 / float64(stat.Blocks*uint64(stat.Bsize)))
	status.Uptime = getUptime()

	memstat := getMemoryStatistic()
	status.MemoryUsage = float64(memstat.Wired+memstat.Active) * 100.0 / float64(getMemsize())
	status.CPUUsage = getCPUUsage()

	fmt.Println("Free space (B) : ", status.HarddiskFreeSpace)
	fmt.Println("Space Usage (%) : ", status.HarddiskUsage)
	fmt.Println("Uptime (s) : ", status.Uptime)
	fmt.Println("Memory Usage (%) : ", status.MemoryUsage)
	fmt.Println("CPU Usage (%) : ", status.CPUUsage)

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
