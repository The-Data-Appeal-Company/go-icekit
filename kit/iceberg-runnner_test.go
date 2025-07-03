package kit

import (
	"context"
	"github.com/stretchr/testify/assert"
	_ "github.com/trinodb/trino-go-client/trino"
	"testing"
)

func Test_ShouldSetupTrinoContainer(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers := icebergRunner.Setup(ctx)

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	icebergRunner.Teardown(ctx, containers)
}

func Test_ShouldSetupTrinoContainerWithCustomTrinoVersion(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers := icebergRunner.SetupWithCustomVersions(ctx, "419", defaultPostgresVersion)

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	icebergRunner.Teardown(ctx, containers)
}

func Test_ShouldSetupTrinoContainerWithCustomPostgresVersion(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers := icebergRunner.SetupWithCustomVersions(ctx, defaultTrinoVersion, "13")

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	icebergRunner.Teardown(ctx, containers)
}

func Test_ShouldSetupTrinoContainerWithCustomVersions(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers := icebergRunner.SetupWithCustomVersions(ctx, "419", "13")

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	icebergRunner.Teardown(ctx, containers)
}
