package dataxr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseArgs(t *testing.T) {
	flags := Config{
		Xms:        "1g",
		Xmx:        "1g",
		Loglevel:   "ERROR",
		DataxHome:  "/tmp/datax",
		Jobid:      "1",
		ConfigFile: "/tmp/datax_test.json",
		Mode:       "standalone",
	}

	expect := []string{
		"-server",
		"-Xms1g",
		"-Xmx1g",
		"-XX:+HeapDumpOnOutOfMemoryError",
		"-XX:HeapDumpPath=/tmp/datax/log",
		"-Dloglevel=ERROR",
		"-Dfile.encoding=UTF-8",
		"-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener",
		"-Djava.security.egd=file:///dev/urandom",
		"-Ddatax.home=/tmp/datax",
		"-Dlogback.configurationFile=/tmp/datax/conf/logback.xml",
		"-classpath",
		"/tmp/datax/lib/*:.",
		"-Dlog.file.name=dlog_1",
		"com.alibaba.datax.core.Engine",
		"-mode",
		"standalone",
		"-jobid",
		"1",
		"-job",
		"/tmp/datax_test.json",
	}

	result, err := parseArgs(flags)
	assert.Nil(t, err)
	assert.Equal(t, expect, result)
}
