package cmdlog

import (
	"time"
	"os/exec"
)

const (
	DefaultPathToFile = "/var/log/docker/scheduler.log"
	ServiceCome       = "[service come  ]"
	ServiceFinish     = "[service finish]"
	ScorePrint        = "[score   print ]"
)

func Write(state string, event string, pathToFile string) {
	t := time.Now().Format(time.UnixDate)
	content := t + "\t" + state + "\t" + event
	command := "echo \"" + content + "\" >> " + pathToFile
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Run()
}
