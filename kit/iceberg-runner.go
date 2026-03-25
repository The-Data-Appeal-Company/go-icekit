package kit

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
)

type IIcebergRunner interface {
	Setup(ctx context.Context) (*IcebergContainer, error)
	SetupWithCustomVersions(ctx context.Context, trinoVersion string, postgresVersion string) (*IcebergContainer, error)
	Teardown(ctx context.Context, containers *IcebergContainer) error
}

var defaultTrinoVersion = "466"
var defaultPostgresVersion = "15"

type IcebergRunner struct{}

func (i IcebergRunner) Setup(ctx context.Context) (*IcebergContainer, error) {
	return i.SetupWithCustomVersions(ctx, defaultTrinoVersion, defaultPostgresVersion)
}

func (i IcebergRunner) SetupWithCustomVersions(ctx context.Context, trinoVersion string, postgresVersion string) (*IcebergContainer, error) {
	icebergContainers, err := CreateTrinoDatabase(ctx, trinoVersion, postgresVersion)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"trino_version":    trinoVersion,
			"postgres_version": postgresVersion,
		}).Error("failed to setup iceberg containers")
		return nil, fmt.Errorf("error creating iceberg container: %w", err)
	}
	return icebergContainers, nil
}

func (i IcebergRunner) Teardown(ctx context.Context, containers *IcebergContainer) error {
	if containers == nil {
		return nil
	}

	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}

	teardownCtx, cancel := context.WithTimeout(baseCtx, 90*time.Second)
	defer cancel()

	err := rollbackSetup(teardownCtx, containers.Db, containers.Network, []resourceTerminator{
		containers.Postgres,
		containers.Minio,
		containers.MinioServer,
		containers.RestIceberg,
		containers.Trino,
	})
	if err != nil {
		logrus.WithError(err).Error("failed to teardown one or more iceberg resources")
	}
	return err
}

func terminateContainer(ctx context.Context, container resourceTerminator, opts ...testcontainers.TerminateOption) error {
	if container == nil {
		return nil
	}

	if len(opts) == 0 {
		return container.Terminate(ctx)
	}

	return container.Terminate(ctx, opts...)
}
