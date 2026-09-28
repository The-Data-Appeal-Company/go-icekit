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
	assert.True(t, containers.ObjectStore.IsRunning())
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
	assert.True(t, containers.ObjectStore.IsRunning())
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
	assert.True(t, containers.ObjectStore.IsRunning())
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
	assert.True(t, containers.ObjectStore.IsRunning())
	assert.True(t, containers.RestIceberg.IsRunning())
	assert.True(t, containers.Trino.IsRunning())

	assert.NoError(t, icebergRunner.Teardown(ctx, containers))
}

func Test_ShouldWriteAndReadIcebergTable(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}
	containers, err := icebergRunner.Setup(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, icebergRunner.Teardown(ctx, containers)) }()

	db := containers.Db
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS iceberg.icekit_test",
		"CREATE TABLE iceberg.icekit_test.items (id bigint, name varchar) WITH (partitioning = ARRAY['name'])",
		"INSERT INTO iceberg.icekit_test.items VALUES (1, 'a'), (2, 'a'), (3, 'b')",
		// row-level delete: writes positional delete files to the object store
		"DELETE FROM iceberg.icekit_test.items WHERE id = 2",
		// rewrites data files and applies the deletes
		"ALTER TABLE iceberg.icekit_test.items EXECUTE optimize",
	} {
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}

	var count, sum int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*), sum(id) FROM iceberg.icekit_test.items").Scan(&count, &sum))
	assert.Equal(t, int64(2), count)
	assert.Equal(t, int64(4), sum)
}

func Test_ObjectStoreImageDefaultsToRustFS(t *testing.T) {
	t.Setenv("ICEKIT_OBJECT_STORE_IMAGE", "")
	assert.Equal(t, "rustfs/rustfs:1.0.0", objectStoreImage())
}

func Test_ObjectStoreImageCanBeOverridden(t *testing.T) {
	t.Setenv("ICEKIT_OBJECT_STORE_IMAGE", "registry.example.com/mirror/rustfs:1.0.0")
	assert.Equal(t, "registry.example.com/mirror/rustfs:1.0.0", objectStoreImage())
}

func Test_ShouldTeardownNilContainersWithoutError(t *testing.T) {
	ctx := context.Background()
	icebergRunner := IcebergRunner{}

	assert.NoError(t, icebergRunner.Teardown(ctx, nil))
}
