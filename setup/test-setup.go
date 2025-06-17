package setup

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path/filepath"
	"time"
)

func CreateTrinoDatabase(ctx context.Context, trinoVersion string) (*IcebergContainer, error) {
	net, err := network.New(ctx, network.WithDriver("bridge"))
	if err != nil {
		return nil, fmt.Errorf("failed to create docker network: %w", err)
	}
	networkName := net.Name

	postgresContainer, err := createPostgresMetastore(ctx, networkName)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}
	state, err := postgresContainer.State(ctx)
	if err != nil || !state.Running {
		return nil, fmt.Errorf("postgres is not running or unhealthy: %v", err)
	}

	minioContainer, err := createMinioContainer(ctx, networkName)
	if err != nil {
		return nil, fmt.Errorf("failed to create minio container: %w", err)
	}

	minioServerContainer, err := createMinioServerContainer(ctx, networkName)
	if err != nil {
		return nil, fmt.Errorf("failed to create minio server container: %w", err)
	}

	restIcebergContainer, err := createRestIcebergCatalogContainer(ctx, networkName)
	if err != nil {
		return nil, fmt.Errorf("failed to create rest-iceberg catalog container: %w", err)
	}

	trinoEnv := map[string]string{
		"AWS_ACCESS_KEY_ID":     "admin",
		"AWS_SECRET_ACCESS_KEY": "password",
		"AWS_REGION":            "us-east-1",
	}

	absPath, err := filepath.Abs("./testdata/catalogs/iceberg.properties")

	trinoImage := fmt.Sprintf("trinodb/trino:%s", trinoVersion)
	tr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Name:     "trino",
			Image:    trinoImage,
			Networks: []string{networkName},
			Env:      trinoEnv,
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{absPath + ":/etc/trino/catalog/iceberg.properties"}
			},
			WaitingFor: wait.ForLog("======== SERVER STARTED ========"),
		},
		Started: false,
	})
	if err != nil {
		return nil, err
	}
	err = tr.Start(ctx)
	time.Sleep(1000 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	ip, err := tr.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := tr.MappedPort(ctx, "8080")
	if err != nil {
		return nil, err
	}
	connection := TrinoConf{
		User:    "PLZ",
		Host:    ip,
		Schema:  "default",
		Catalog: "iceberg",
		Port:    port.Int(),
	}
	db, err := sql.Open("trino", connection.ConnectionString())
	if err != nil {
		return nil, err
	}
	icebergContainers := IcebergContainer{
		Trino:       tr,
		Db:          db,
		Postgres:    postgresContainer,
		Minio:       minioContainer,
		MinioServer: minioServerContainer,
		RestIceberg: restIcebergContainer,
	}
	return &icebergContainers, nil
}

func createMinioContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	// dir to be used as minio volume
	tempDir, err := os.MkdirTemp("", "minio-data")
	if err != nil {
		return nil, err
	}

	minioEnv := map[string]string{
		"MINIO_ROOT_USER":     "admin",
		"MINIO_ROOT_PASSWORD": "password",
		"MINIO_DOMAIN":        "minio",
	}

	req := testcontainers.ContainerRequest{
		Name:         "minio",
		Image:        "minio/minio:RELEASE.2025-05-24T17-08-30Z",
		ExposedPorts: []string{"9000/tcp"},
		Env:          minioEnv,
		Networks:     []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"warehouse.minio"},
		},
		Cmd:        []string{"server", "/data", "--console-address", ":9001"},
		WaitingFor: wait.ForListeningPort("9000/tcp"),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = []string{tempDir + ":/data"}
		},
	}

	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return minioContainer, nil
}

func createMinioServerContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	mcEnv := map[string]string{
		"AWS_ACCESS_KEY_ID":     "admin",
		"AWS_SECRET_ACCESS_KEY": "password",
		"AWS_REGION":            "us-east-1",
	}
	mcCmd := []string{
		"/bin/sh", "-c",
		"until (/usr/bin/mc alias set minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;" +
			"/usr/bin/mc mb --ignore-existing minio/warehouse;" +
			"/usr/bin/mc policy set public minio/warehouse;" +
			"tail -f /dev/null",
	}

	req := testcontainers.ContainerRequest{
		Name:       "mc",
		Image:      "minio/mc:RELEASE.2025-05-21T01-59-54Z.hotfix.e98f1ead",
		Networks:   []string{networkName},
		Env:        mcEnv,
		Entrypoint: mcCmd,
	}

	minioServerContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return minioServerContainer, nil
}

func createPostgresMetastore(ctx context.Context, networkName string) (testcontainers.Container, error) {
	// dir to be used as postgres volume
	tempDir, err := os.MkdirTemp("", "postgres_data")
	if err != nil {
		return nil, err
	}

	postgresEnv := map[string]string{
		"PGDATA":                    "/var/lib/postgresql/data",
		"POSTGRES_USER":             "admin",
		"POSTGRES_PASSWORD":         "password",
		"POSTGRES_DB":               "demo_catalog",
		"POSTGRES_HOST_AUTH_METHOD": "md5",
	}

	req := testcontainers.ContainerRequest{
		Name:     "postgres",
		Image:    "postgres:15",
		Env:      postgresEnv,
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"postgres"},
		},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432"),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = []string{tempDir + ":/var/lib/postgresql/data"}
		},
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return pgContainer, nil
}

func createRestIcebergCatalogContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	catalogUri := fmt.Sprintf("jdbc:postgresql://postgres:5432/demo_catalog")
	restEnv := map[string]string{
		"AWS_ACCESS_KEY_ID":     "admin",
		"AWS_SECRET_ACCESS_KEY": "password",
		"AWS_REGION":            "us-east-1",
		"CATALOG_WAREHOUSE":     "s3://warehouse/",
		"CATALOG_IO__IMPL":      "org.apache.iceberg.aws.s3.S3FileIO",
		"CATALOG_S3_ENDPOINT":   "http://minio:9000",
		"CATALOG_URI":           catalogUri,
		"CATALOG_JDBC_USER":     "admin",
		"CATALOG_JDBC_PASSWORD": "password",
	}

	req := testcontainers.ContainerRequest{
		Name:         "iceberg-rest",
		Image:        "tabulario/iceberg-rest:1.6.0",
		Env:          restEnv,
		Networks:     []string{networkName},
		ExposedPorts: []string{"8181/tcp"},
		WaitingFor:   wait.ForLog("Started").WithStartupTimeout(30 * time.Second),
	}

	icebergCatalogContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return icebergCatalogContainer, nil
}
