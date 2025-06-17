package setup

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

func Test_ShouldSetupTrinoContainerWithCustomVersion(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers := icebergRunner.SetupWithCustomTrinoVersion(ctx, "419")

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	icebergRunner.Teardown(ctx, containers)
}
