package user

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
)

type LinuxUser struct {
	*user.User
	Groups string
	Passwd string
}

func GetUsers() ([]*LinuxUser, error) {
	cmd := exec.Command("awk", "-F:", `($3>=1000) && ($3!=65534) {print $3}`, "/etc/passwd")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("Get user id error %s %v", out, err)
	}
	uids := strings.Split(string(out), "\n")
	uids = uids[:len(uids)-1]
	users := make([]*LinuxUser, len(uids))
	for i, uid := range uids {
		u, err := user.LookupId(uid)
		if err != nil {
			log.Println("Error: cannot find user id", uid, err)
			continue
		}
		cmd = exec.Command("awk", "-F:", fmt.Sprintf(`($1~/%s/) {print $2}`, u.Username), "/etc/shadow")
		p, err := cmd.CombinedOutput()
		if err != nil {
			log.Println("Error: cannot get user passswd", u.Username, err)
		}

		cmd = exec.Command("groups", u.Username)
		out, err = cmd.CombinedOutput()
		if err != nil {
			log.Println("Error: cannot get user group", u.Username, err)
		}
		g := strings.Split(string(out), ":")
		linuxUser := &LinuxUser{u, g[len(g)-1], string(p)}
		users[i] = linuxUser
	}
	return users, nil
}

func AddUser(u *LinuxUser) error {
	cmd := exec.Command("useradd",
		"-d", u.HomeDir,
		"-g", u.Gid, "-G", u.Groups,
		"-p", u.Passwd,
		"-u", u.Uid, u.Username)
	_, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Error: add user %s %v", u.Username, err)
	}
	if strings.Contains(u.Groups, "gpuusers") {
		bashrcPath := filepath.Join(u.HomeDir, ".bashrc")
		f, err := os.OpenFile(bashrcPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return fmt.Errorf("OpenFile Error: %v", err)
		}
		f.WriteString("DISPLAY=:0 sudo /usr/bin/xhost + 1> /dev/null 2>&1")
		f.Close()
	}
	return nil
}
