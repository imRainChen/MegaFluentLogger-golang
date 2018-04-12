package fluentd

import (
	"log"
	"fmt"
	"fluent-logger-golang/fluentd/diskqueue"
)

const (
	DEBUG = diskqueue.DEBUG
	INFO  = diskqueue.INFO
	WARN  = diskqueue.WARN
	ERROR = diskqueue.ERROR
	FATAL = diskqueue.FATAL
)

func NewLogger() diskqueue.AppLogFunc {
	return func(lvl diskqueue.LogLevel, f string, args ...interface{})  {
		log.Println(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
}