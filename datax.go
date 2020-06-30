// Package dataxr is a lib for alibaba datax

package dataxr

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	Debug      bool   `json:"debug"`
	Xms        string `json:"xms"`
	Xmx        string `json:"xmx"`
	Loglevel   string `json:"loglevel"`
	DataxHome  string `json:"datax_home"`
	Mode       string `json:"mode"`
	Jobid      string `json:"jobid"`
	ConfigFile string `json:"config_file"`
	// Writer View job config[reader] template, eg: mysqlreader,streamreader
	Writer string `json:"writer"`
	// Reader View job config[writer] template, eg: mysqlwriter,streamwriter
	Reader string `json:"reader"`
}

type LogLine interface {
	Write(text string)
}

type DataX struct {
	cmd              *exec.Cmd
	quit             chan error
	stdout           io.ReadCloser
	stderr           io.ReadCloser
	cfg              Config
	stdoutLog        LogLine
	stderrLog        LogLine
	logPipeWaitGroup sync.WaitGroup
}

func NewDataX(cfg Config, stdoutLog LogLine, stderrLog LogLine) *DataX {
	return &DataX{
		cfg:              cfg,
		quit:             make(chan error, 1),
		stdoutLog:        stdoutLog,
		stderrLog:        stderrLog,
		logPipeWaitGroup: sync.WaitGroup{},
	}
}

func (d *DataX) Kill() error {
	return d.cmd.Process.Kill()
}

func (d *DataX) dispose() {
	if d.stdout != nil {
		err := d.stdout.Close()
		if err != nil {
			log.Print(err)
		}
	}

	if d.stderr != nil {
		err := d.stderr.Close()
		if err != nil {
			log.Print(err)
		}
	}

	if err := d.Kill(); err != nil {
		log.Print(err)
	}
}

func (d *DataX) Wait(ctx context.Context, timeout time.Duration) error {
	go func() {
		d.logPipeWaitGroup.Wait()
		d.quit <- d.cmd.Wait()
	}()

	select {
	case <-time.After(timeout):
		d.dispose()
		return fmt.Errorf("timeout after %f seconds", timeout.Seconds())
	case <-ctx.Done():
		d.dispose()
		return ctx.Err()
	case err := <-d.quit:
		return err
	}
}

func (d *DataX) Exec(ctx context.Context, program string) (pid int, err error) {
	args, err := parseArgs(d.cfg)
	if err != nil {
		return
	}

	cmd := exec.CommandContext(ctx, program, args...)
	if d.cfg.Debug {
		log.Println(cmd.String())
		log.Printf("%+v\n", args)
	}
	d.cmd = cmd

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return
	}
	d.stderr = stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}
	d.stdout = stdout

	err = cmd.Start()
	if err != nil {
		return
	}

	pid = cmd.Process.Pid
	d.logPipeWaitGroup.Add(2)

	go func() {
		if err := d.bindPipStdLog(d.stdoutLog, stdout); err != nil {
			log.Printf("%+v", err)
		}
	}()

	go func() {
		if err := d.bindPipStdLog(d.stderrLog, stderr); err != nil {
			log.Printf("%+v", err)
		}
	}()

	return
}

func (d *DataX) bindPipStdLog(logger LogLine, stdPip io.Reader) error {
	defer d.logPipeWaitGroup.Done()

	scanner := bufio.NewScanner(stdPip)
	for scanner.Scan() {
		line := scanner.Text()
		logger.Write(line)
	}

	return scanner.Err()
}

func parseArgs(cfg Config) ([]string, error) {

	logLevel := "info"
	mode := "standalone"

	if cfg.Loglevel != "" {
		logLevel = cfg.Loglevel
	}

	if cfg.Mode != "" {
		mode = cfg.Mode
	}

	dataxHome, err := filepath.Abs(cfg.DataxHome)
	if err != nil {
		return nil, err
	}

	job, err := filepath.Abs(cfg.ConfigFile)
	if err != nil {
		return nil, err
	}

	args := []string{
		"-server",
		"-Xms" + cfg.Xms,
		"-Xmx" + cfg.Xmx,
		"-XX:+HeapDumpOnOutOfMemoryError",
		"-XX:HeapDumpPath=" + dataxHome + "/log",
		"-Dloglevel=" + logLevel,
		"-Dfile.encoding=UTF-8",
		"-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener",
		"-Djava.security.egd=file:///dev/urandom",
		"-Ddatax.home=" + dataxHome,
		"-Dlogback.configurationFile=" + dataxHome + "/conf/logback.xml",
		"-classpath",
		dataxHome + "/lib/*:.",
		"-Dlog.file.name=dlog_" + cfg.Jobid,
		"com.alibaba.datax.core.Engine",
		"-mode",
		mode,
		"-jobid",
		cfg.Jobid,
		"-job",
		job,
	}

	return args, nil
}
