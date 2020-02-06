# datax-runner

一个 lib,方便 Go 语言调起[datax](https://github.com/alibaba/datax)

## Why datax-runner

因为官方只提供了一个`python`调用的脚本

## 示例

```go
import (
	"context"
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
	datax := dataxr.NewDataX(dataxr.Config{
		Debug:      true,
		Xms:        "512m",
		Xmx:        "512m",
		Loglevel:   "error",
		DataxHome:  "./datax",
		Mode:       "",
		Jobid:      "1",
		ConfigFile: "./datax_stream_job.json",
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
```
