package main

/*
使用dataax-runner运行datax示例:

1. 下载datax,解压到当前的example目录下
2. cd example && go run start.go
*/

import (
	"context"
	"log"
	"time"

	dataxr "github.com/koolay/datax-runner"
)

type ExecLog struct {
}

func (lg *ExecLog) Write(text string) {
	log.Println(text)
}

func main() {
	datax := dataxr.NewDataX(dataxr.Config{
		Debug:      true,
		Xms:        "512m",
		Xmx:        "512m",
		Loglevel:   "error",
		DataxHome:  "./datax",
		Mode:       "",
		Jobid:      "1",
		ConfigFile: "./datax_stream_job.json",
	}, &ExecLog{}, &ExecLog{})

	ctx := context.Background()

	pid, err := datax.Exec(ctx, "java")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("pid", pid)
	err = datax.Wait(ctx, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}
}
