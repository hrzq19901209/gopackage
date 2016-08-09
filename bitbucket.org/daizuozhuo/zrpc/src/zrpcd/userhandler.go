package main

import (
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"

	"bitbucket.org/daizuozhuo/zrpc/src/config"
	"bitbucket.org/daizuozhuo/zrpc/src/protocol"
	"bitbucket.org/daizuozhuo/zrpc/src/user"
)

func usersHandler(rw http.ResponseWriter, request *http.Request) {
	users, err := user.GetUsers()
	content := struct {
		Users []*user.LinuxUser
		Err   error
	}{users, err}
	staticDir := config.GetConfig().StaticDir
	t := template.Must(template.ParseFiles(
		filepath.Join(staticDir, "template/header.html"),
		filepath.Join(staticDir, "template/footer.html"),
		filepath.Join(staticDir, "template/users.html")))
	t.ExecuteTemplate(rw, "main", content)
}

func syncUser(rw http.ResponseWriter, request *http.Request) {
	users, err := user.GetUsers()
	if err != nil {
		rw.Write([]byte(fmt.Sprintf("fail, get users error %v", err)))
		return
	}
	out, err := manager.ExecuteAll("Worker.AddUsers", users)
	if err != nil {
		rw.Write([]byte(fmt.Sprintf("fail, sync users error %s %v", out, err)))
		return
	}
	rw.Write([]byte("sucess"))
}

func userHandler(rw http.ResponseWriter, request *http.Request) {
	userMasters := make(map[string]*protocol.Master)
	user := request.URL.Path[len("/user/"):]
	for id, master := range GlobalMasters.data {
		if master.GetUser() == user {
			userMasters[id] = master
		}
	}
	content := &IndexContent{Masters: userMasters, Manager: manager}
	staticDir := config.GetConfig().StaticDir
	t := template.Must(template.ParseFiles(
		filepath.Join(staticDir, "template/header.html"),
		filepath.Join(staticDir, "template/footer.html"),
		filepath.Join(staticDir, "template/index.html")))
	t.ExecuteTemplate(rw, "main", content)
}
