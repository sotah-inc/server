package logging

import (
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

// AddHook adds a hook to the internal logrus instance
func AddHook(hook logrus.Hook) {
	logger.Hooks.Add(hook)
}

// Info logs
func Info(args ...interface{}) {
	logger.Info(args...)
}

// Debug logs
func Debug(args ...interface{}) {
	logger.Debug(args...)
}

// Error logs
func Error(args ...interface{}) {
	logger.Error(args...)
}
