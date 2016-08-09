package zsystem

type SystemStatus struct {
	Name              string
	CPUUsage          float64
	MemoryUsage       float64
	MemoryFreeSpace   float64
	Uptime            float64
	HarddiskUsage     float64
	HarddiskFreeSpace uint64
	IP                string
}

type MemoryStatistic struct {
	Free     uint64
	Inactive uint64
	Active   uint64
	Wired    uint64
	Total    uint64
}

func GetStatus() *SystemStatus {
	status := nativeStatus()
	return status
}

func Copy(src string, dst string) {
	copy(src, dst)
}

func Move(src string, dst string) {
	move(src, dst)
}
