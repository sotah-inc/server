package stackdriver

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/errorreporting"
	stackdriverlogging "cloud.google.com/go/logging"
	"github.com/sirupsen/logrus"
)

func NewHook(projectID string, serviceName string) (Hook, error) {
	ctx := context.Background()

	lc, err := stackdriverlogging.NewClient(ctx, fmt.Sprintf("projects/%s", projectID))
	if err != nil {
		return Hook{}, err
	}

	ec, err := errorreporting.NewClient(ctx, projectID, errorreporting.Config{
		ServiceName:    serviceName,
		ServiceVersion: "v1.0",
	})
	if err != nil {
		return Hook{}, err
	}

	return Hook{lc, ec, lc.Logger("sotah-server"), ctx}, nil
}

type Hook struct {
	loggingClient        *stackdriverlogging.Client
	errorReportingClient *errorreporting.Client
	logger               *stackdriverlogging.Logger
	ctx                  context.Context
}

func (h Hook) Fire(entry *logrus.Entry) error {
	switch entry.Level {
	case logrus.PanicLevel:
		err := h.errorReportingClient.ReportSync(
			h.ctx,
			errorreporting.Entry{Error: errors.New(entry.Message)},
		)
		if err != nil {
			return err
		}

		return h.logger.LogSync(
			h.ctx,
			newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Emergency),
		)
	case logrus.FatalLevel:
		err := h.errorReportingClient.ReportSync(
			h.ctx,
			errorreporting.Entry{Error: errors.New(entry.Message)},
		)
		if err != nil {
			return err
		}

		return h.logger.LogSync(
			h.ctx,
			newStackdriverEntryFromLogrusEntry(entry, stackdriverlogging.Critical),
		)
	case logrus.ErrorLevel:
		h.errorReportingClient.Report(errorreporting.Entry{Error: errors.New(entry.Message)})

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
	payload := map[string]interface{}{}
	for k, v := range e.Data {
		payload[k] = v
	}
	payload["msg"] = e.Message

	return stackdriverlogging.Entry{
		Timestamp: e.Time,
		Payload:   payload,
		Severity:  severity,
	}
}
