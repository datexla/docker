package cmdlog

import (
	"time"
	"os/exec"
)

func write(content string, pathToFile string) {
	t := time.Now().Format(time.UnixDate)
	command := "echo \"" + t + " " + content + "\" >> " + pathToFile
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Run()
}