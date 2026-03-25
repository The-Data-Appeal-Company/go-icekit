package kit

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/trinodb/trino-go-client/trino"
	"testing"
)

func Test_ShouldSetupTrinoContainer(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers, err := icebergRunner.Setup(ctx)
	require.NoError(t, err)

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	assert.NoError(t, icebergRunner.Teardown(ctx, containers))
}

func Test_ShouldSetupTrinoContainerWithCustomTrinoVersion(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers, err := icebergRunner.SetupWithCustomVersions(ctx, "419", defaultPostgresVersion)
	require.NoError(t, err)

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	assert.NoError(t, icebergRunner.Teardown(ctx, containers))
}

func Test_ShouldSetupTrinoContainerWithCustomPostgresVersion(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers, err := icebergRunner.SetupWithCustomVersions(ctx, defaultTrinoVersion, "13")
	require.NoError(t, err)

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	assert.NoError(t, icebergRunner.Teardown(ctx, containers))
}

func Test_ShouldSetupTrinoContainerWithCustomVersions(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers, err := icebergRunner.SetupWithCustomVersions(ctx, "419", "13")
	require.NoError(t, err)

	assert.True(t, containers.Postgres.IsRunning())
	assert.True(t, containers.Minio.IsRunning())
	assert.True(t, containers.MinioServer.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	assert.NoError(t, icebergRunner.Teardown(ctx, containers))
}

func Test_ShouldTeardownNilContainersWithoutError(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}

	assert.NoError(t, icebergRunner.Teardown(ctx, nil))
}
