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
	cmd       *exec.Cmd
	quit      chan error
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	cfg       Config
	stdoutLog LogLine
	stderrLog LogLine
}

func NewDataX(cfg Config, stdoutLog LogLine, stderrLog LogLine) *DataX {
	return &DataX{
		cfg:       cfg,
		quit:      make(chan error, 1),
		stdoutLog: stdoutLog,
		stderrLog: stderrLog,
	}
}

func (d *DataX) Kill() error {
	d.dispose()
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
}

func (d *DataX) Wait(ctx context.Context, timeout time.Duration) error {

	go func() {
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

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			d.stderrLog.Write(line)
		}

		if err := scanner.Err(); err != nil {
			cmd.Process.Kill()
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			d.stdoutLog.Write(line)
		}
		if err := scanner.Err(); err != nil {
			cmd.Process.Kill()
			d.quit <- err
		}
	}()

	return
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
