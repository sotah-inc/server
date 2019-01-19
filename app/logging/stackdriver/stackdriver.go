package stackdriver

import (
	"context"
	"fmt"

	stackdriverlogging "cloud.google.com/go/logging"
	"github.com/sirupsen/logrus"
)

func NewHook(projectID string) (Hook, error) {
	ctx := context.Background()
	client, err := stackdriverlogging.NewClient(ctx, fmt.Sprintf("projects/%s", projectID))
	if err != nil {
		return Hook{}, err
	}

	return Hook{client, client.Logger("sotah-server"), ctx}, nil
}

type Hook struct {
	client *stackdriverlogging.Client
	logger *stackdriverlogging.Logger
	ctx    context.Context
}

func (h Hook) Fire(entry *logrus.Entry) error {
	switch entry.Level {
	case logrus.PanicLevel:
		return h.logger.LogSync(
			h.ctx,
			newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Emergency),
		)
	case logrus.FatalLevel:
		return h.logger.LogSync(
			h.ctx,
			newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Critical),
		)
	case logrus.ErrorLevel:
		h.logger.Log(newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Error))

		return nil
	case logrus.WarnLevel:
		h.logger.Log(newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Warning))

		return nil
	case logrus.InfoLevel:
		h.logger.Log(newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Info))

		return nil
	case logrus.DebugLevel:
		h.logger.Log(newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Debug))

		return nil
	default:
		return nil
	}
}

func (h Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func newStackdriverEntryFromLogrusEntry(e *logrus.Entry, severity stackdriverlogging.Severity) stackdriverlogging.Entry {
	e.Data["_message"] = e.Message

	return stackdriverlogging.Entry{
		Timestamp: e.Time,
		Payload:   e.Data,
		Severity:  severity,
	}
}
