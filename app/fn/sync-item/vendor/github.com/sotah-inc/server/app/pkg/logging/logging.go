package logging

import (
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

// ResetLogger re-establishes the logger instance
func ResetLogger(level logrus.Level, hook logrus.Hook) {
	logger = logrus.New()
	logger.SetLevel(level)
	logger.AddHook(hook)
}

// AddHook adds a hook to the internal logrus instance
func AddHook(hook logrus.Hook) {
	logger.Hooks.Add(hook)
}

// SetLevel sets log level
func SetLevel(level logrus.Level) {
	logger.SetLevel(level)
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

// Fatal logs
func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

// Fatalf logs
func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

// WithField logs
func WithField(key string, value interface{}) *logrus.Entry {
	return logger.WithField(key, value)
}

// WithFields logs
func WithFields(fields logrus.Fields) *logrus.Entry {
	return logger.WithFields(fields)
}
