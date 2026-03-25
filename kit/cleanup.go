package kit

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
)

type resourceTerminator interface {
	Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
}

type networkRemover interface {
	Remove(ctx context.Context) error
}

type resourceCloser interface {
	Close() error
}

func rollbackSetup(ctx context.Context, db resourceCloser, net networkRemover, resources []resourceTerminator) error {
	var cleanupErr error

	if db != nil {
		if err := db.Close(); err != nil {
			logrus.WithError(err).Error("failed to close trino database during cleanup")
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}

	for idx := len(resources) - 1; idx >= 0; idx-- {
		if err := terminateContainer(ctx, resources[idx]); err != nil {
			logrus.WithError(err).WithField("resource_index", idx).Error("failed to terminate resource during cleanup")
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}

	if net != nil {
		if err := net.Remove(ctx); err != nil {
			logrus.WithError(err).Error("failed to remove docker network during cleanup")
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}

	return cleanupErr
}
