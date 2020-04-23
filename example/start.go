package main

/*
使用dataax-runner运行datax示例:

1. 下载datax,解压到当前的example目录下
2. cd example && go run start.go
*/

import (
	"context"
	"flag"
	"log"
	"time"

	dataxr "github.com/koolay/datax-runner"
)

type StdoutLog struct {
}

type StderrLog struct {
}

func (lg *StdoutLog) Write(text string) {
	log.Println("[Stdout]", text)
}

func (lg *StderrLog) Write(text string) {
	log.Println("[Stderr]", text)
}

func main() {
	var cfgFilePath string
	flag.StringVar(&cfgFilePath, "config", "./datax_stream_job.json", "job config file")
	flag.Parse()

	datax := dataxr.NewDataX(dataxr.Config{
		Debug:      true,
		Xms:        "512m",
		Xmx:        "512m",
		Loglevel:   "error",
		DataxHome:  "./datax",
		Mode:       "",
		Jobid:      "1",
		ConfigFile: cfgFilePath,
	}, &StdoutLog{}, &StderrLog{})

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
