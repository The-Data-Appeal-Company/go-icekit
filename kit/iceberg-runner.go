package kit

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
	"os"
)

type IIcebergRunner interface {
	Setup(ctx context.Context) *IcebergContainer
	SetupWithCustomVersions(ctx context.Context, trinoVersion string, postgresVersion string) *IcebergContainer
	Teardown(ctx context.Context, containers *IcebergContainer)
}

var defaultTrinoVersion = "466"
var defaultPostgresVersion = "15"

type IcebergRunner struct{}

func (i IcebergRunner) Setup(ctx context.Context) *IcebergContainer {
	return i.SetupWithCustomVersions(ctx, defaultTrinoVersion, defaultPostgresVersion)
}

func (i IcebergRunner) SetupWithCustomVersions(ctx context.Context, trinoVersion string, postgresVersion string) *IcebergContainer {
	icebergContainers, err := CreateTrinoDatabase(ctx, trinoVersion, postgresVersion)
	if err != nil {
		logrus.Error("Error creating iceberg container")
		logrus.Error(err)
		os.Exit(1)
	}
	return icebergContainers
}

func (i IcebergRunner) Teardown(ctx context.Context, containers *IcebergContainer) {
	defer func(opts ...testcontainers.TerminateOption) {
		terminateContainer(ctx, containers.Trino, opts...)
		err := containers.Db.Close()
		if err != nil {
			logrus.Error(err)
		}
		terminateContainer(ctx, containers.Postgres, opts...)
		terminateContainer(ctx, containers.Minio, opts...)
		terminateContainer(ctx, containers.MinioServer, opts...)
		terminateContainer(ctx, containers.RestIceberg, opts...)
	}()
}

func terminateContainer(ctx context.Context, container testcontainers.Container, opts ...testcontainers.TerminateOption) {
	err := container.Terminate(ctx, opts...)
	if err != nil {
		logrus.Error(err)
	}
}
