package kit

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func CreateTrinoDatabase(ctx context.Context, trinoVersion string, postgresVersion string) (*IcebergContainer, error) {
	setupCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()

	net, err := network.New(setupCtx, network.WithDriver("bridge"))
	if err != nil {
		return nil, fmt.Errorf("failed to create docker network: %w", err)
	}
	networkName := net.Name
	startedContainers := make([]resourceTerminator, 0, 5)

	var trinoDB *sql.DB
	setupComplete := false
	defer func() {
		if setupComplete {
			return
		}

		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer rollbackCancel()

		if err := rollbackSetup(rollbackCtx, trinoDB, net, startedContainers); err != nil {
			logrus.WithError(err).Error("failed rollback after setup error")
		}
	}()

	postgresContainer, err := createPostgresMetastore(setupCtx, networkName, postgresVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}
	startedContainers = append(startedContainers, postgresContainer)

	state, err := postgresContainer.State(setupCtx)
	if err != nil || !state.Running {
		return nil, fmt.Errorf("postgres is not running or unhealthy: %v", err)
	}

	objectStoreContainer, err := createObjectStoreContainer(setupCtx, networkName)
	if err != nil {
		return nil, fmt.Errorf("failed to create object store container: %w", err)
	}
	startedContainers = append(startedContainers, objectStoreContainer)

	if err := createBucket(setupCtx, objectStoreContainer, warehouseBucket); err != nil {
		return nil, fmt.Errorf("failed to create %s bucket: %w", warehouseBucket, err)
	}

	restIcebergContainer, err := createRestIcebergCatalogContainer(setupCtx, networkName)
	if err != nil {
		return nil, fmt.Errorf("failed to create rest-iceberg catalog container: %w", err)
	}
	startedContainers = append(startedContainers, restIcebergContainer)

	trinoEnv := map[string]string{
		"AWS_ACCESS_KEY_ID":     "admin",
		"AWS_SECRET_ACCESS_KEY": "password",
		"AWS_REGION":            "us-east-1",
	}

	absPathTrinoConf := filepath.Join(getCurrentDir(), "../", "catalogs", "iceberg.properties")
	absPathPgConf := filepath.Join(getCurrentDir(), "../", "catalogs", "postgresql.properties")

	trinoImage := fmt.Sprintf("trinodb/trino:%s", trinoVersion)
	tr, err := testcontainers.GenericContainer(setupCtx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        trinoImage,
			Networks:     []string{networkName},
			Env:          trinoEnv,
			ExposedPorts: []string{"8080/tcp"},
			NetworkAliases: map[string][]string{
				networkName: []string{"trino"},
			},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					absPathTrinoConf + ":/etc/trino/catalog/iceberg.properties",
					absPathPgConf + ":/etc/trino/catalog/postgresql.properties",
				}
			},
			WaitingFor: wait.ForLog("======== SERVER STARTED ========").
				WithStartupTimeout(4 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start trino: %w", err)
	}
	startedContainers = append(startedContainers, tr)

	ip, err := tr.Host(setupCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve trino host: %w", err)
	}
	port, err := tr.MappedPort(setupCtx, "8080/tcp")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve trino port: %w", err)
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
		return nil, fmt.Errorf("failed to open trino connection: %w", err)
	}
	trinoDB = db
	if err := waitForTrinoConnection(setupCtx, db, 90*time.Second); err != nil {
		return nil, fmt.Errorf("trino is not ready for connections: %w", err)
	}

	icebergContainers := IcebergContainer{
		Trino:       tr,
		Db:          db,
		Postgres:    postgresContainer,
		ObjectStore: objectStoreContainer,
		RestIceberg: restIcebergContainer,
		Network:     net,
	}
	setupComplete = true
	return &icebergContainers, nil
}

const warehouseBucket = "warehouse"

// defaultObjectStoreImage is the S3-compatible server that stores the Iceberg data. MinIO withdrew its
// public images in 2026, so the kit uses RustFS. ICEKIT_OBJECT_STORE_IMAGE overrides it, e.g. with a
// mirror in a private registry.
const defaultObjectStoreImage = "rustfs/rustfs:1.0.0"

func objectStoreImage() string {
	if image := os.Getenv("ICEKIT_OBJECT_STORE_IMAGE"); image != "" {
		return image
	}
	return defaultObjectStoreImage
}

func createObjectStoreContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	objectStoreEnv := map[string]string{
		"RUSTFS_ACCESS_KEY": "admin",
		"RUSTFS_SECRET_KEY": "password",
		// serve virtual-hosted-style requests (<bucket>.minio), used by the rest catalog
		"RUSTFS_SERVER_DOMAINS":         "minio",
		"RUSTFS_OBS_LOG_STDOUT_ENABLED": "true",
	}

	req := testcontainers.ContainerRequest{
		Image:        objectStoreImage(),
		ExposedPorts: []string{"9000/tcp"},
		Env:          objectStoreEnv,
		Networks:     []string{networkName},
		// the "minio" host names are kept so that the catalog configurations stay the same
		NetworkAliases: map[string][]string{
			networkName: []string{"minio", warehouseBucket + ".minio"},
		},
		WaitingFor: wait.ForHTTP("/health/ready").WithPort("9000/tcp").
			WithStartupTimeout(2 * time.Minute),
	}

	objectStoreContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return objectStoreContainer, nil
}

// createBucket creates the bucket, if missing, with a SigV4-signed request sent from inside the object
// store container, so no extra container or S3 client is needed.
func createBucket(ctx context.Context, objectStore testcontainers.Container, bucket string) error {
	url := fmt.Sprintf("http://localhost:9000/%s", bucket)
	script := fmt.Sprintf(
		"curl -sf -o /dev/null --aws-sigv4 aws:amz:us-east-1:s3 --user admin:password --head %[1]s || "+
			"curl -sSf --aws-sigv4 aws:amz:us-east-1:s3 --user admin:password -X PUT %[1]s", url)

	code, output, err := objectStore.Exec(ctx, []string{"sh", "-c", script}, tcexec.Multiplexed())
	if err != nil {
		return err
	}
	if code != 0 {
		msg, _ := io.ReadAll(output)
		return fmt.Errorf("exit code %d: %s", code, strings.TrimSpace(string(msg)))
	}
	return nil
}

func createPostgresMetastore(ctx context.Context, networkName string, postgresVersion string) (testcontainers.Container, error) {
	postgresEnv := map[string]string{
		"PGDATA":                    "/var/lib/postgresql/data",
		"POSTGRES_USER":             "admin",
		"POSTGRES_PASSWORD":         "password",
		"POSTGRES_DB":               "demo_catalog",
		"POSTGRES_HOST_AUTH_METHOD": "md5",
	}

	postgresImage := fmt.Sprintf("postgres:%s", postgresVersion)
	req := testcontainers.ContainerRequest{
		Image:        postgresImage,
		Env:          postgresEnv,
		Networks:     []string{networkName},
		ExposedPorts: []string{"5432/tcp"},
		NetworkAliases: map[string][]string{
			networkName: []string{"postgres"},
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithStartupTimeout(2 * time.Minute),
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
		Image:    "tabulario/iceberg-rest:1.6.0",
		Env:      restEnv,
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: []string{"iceberg-rest"},
		},
		ExposedPorts: []string{"8181/tcp"},
		WaitingFor:   wait.ForListeningPort("8181/tcp").WithStartupTimeout(2 * time.Minute),
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

func getCurrentDir() string {
	_, filename, _, _ := runtime.Caller(1)
	return filepath.Dir(filename)
}

func waitForTrinoConnection(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		// a ping succeeds as soon as the coordinator answers, before any node can run a query:
		// wait for a real query instead
		var one int
		if err := db.QueryRowContext(deadlineCtx, "SELECT 1").Scan(&one); err == nil {
			return nil
		}

		select {
		case <-deadlineCtx.Done():
			return deadlineCtx.Err()
		case <-ticker.C:
		}
	}
}
