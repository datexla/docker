package cmdlog

import (
	"time"
	"os/exec"
)

var (
	defaultPathToFile = "/var/log/docker/scheduler.log"
	serviceCome       = "[service come  ]"
	serviceFinish     = "[service finish]"
	scorePrint        = "[score   print ]"
)

func Write(state string, event string, pathToFile string) {
	t := time.Now().Format(time.UnixDate)
	content := t + "\t" + state + "\t" + event
	command := "echo \"" + content + "\" >> " + pathToFile
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Run()
}
