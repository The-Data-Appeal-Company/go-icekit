package setup

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
	"os"
)

type IIcebergRunner interface {
	Setup(ctx context.Context) *IcebergContainer
	SetupWithCustomTrinoVersion(ctx context.Context, trinoVersion string) *IcebergContainer
	Teardown(ctx context.Context, containers *IcebergContainer)
}

var defaultTrinoVersion = "466"

type IcebergRunner struct{}

func (i IcebergRunner) Setup(ctx context.Context) *IcebergContainer {
	return i.SetupWithCustomTrinoVersion(ctx, defaultTrinoVersion)
}

func (i IcebergRunner) SetupWithCustomTrinoVersion(ctx context.Context, trinoVersion string) *IcebergContainer {
	icebergContainers, err := CreateTrinoDatabase(ctx, trinoVersion)
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
