package cmdlog

import (
	"os/exec"
)

const (
	DefaultPathToFile = "/var/log/docker/scheduler.log"
	ServiceCome       = "[service  come  ]"
	ServiceFinish     = "[service  finish]"
	ScorePrint        = "[score    print ]"
	ScheduleFinish    = "[schedule finish]"
	ManagerInfo       = "[Manager  Info  ]"
	Debug             = "[debug          ]"
)

func Write(state string, event string, pathToFile string) {
	content := "$(date +'%F %T.%3N') " + state + " " + event
	command := "echo \"" + content + "\" >> " + pathToFile
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Run()
}
