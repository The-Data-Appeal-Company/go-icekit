# go-icekit

[![Go Report Card](https://goreportcard.com/badge/github.com/The-Data-Appeal-Company/go-icekit)](https://goreportcard.com/report/github.com/The-Data-Appeal-Company/go-icekit)
![Go](https://github.com/The-Data-Appeal-Company/go-icekit/workflows/Go/badge.svg?branch=master)
[![license](https://img.shields.io/github/license/The-Data-Appeal-Company/go-icekit.svg)](LICENSE)

### Simple configurator for trino iceberg catalog in test containers
go-icekit configures iceberg catalog for trino using test containers. It uses:
* Postgres as metastore
* Minio and Minio server to emulate amazon s3
* Rest iceberg catalog

Also, there is postgresql catalog configuration, to perform trino queries on postgres tables, using metastore container.

### Usage
In go.mod file add dependency

```
require(
    github.com/The-Data-Appeal-Company/go-icekit v0.1.0
)
```

To use it add the following import

```go
import "github.com/The-Data-Appeal-Company/go-icekit/kit"
```

Current API signatures:

```go
func (i IcebergRunner) Setup(ctx context.Context) (*IcebergContainer, error)
func (i IcebergRunner) SetupWithCustomVersions(ctx context.Context, trinoVersion string, postgresVersion string) (*IcebergContainer, error)
func (i IcebergRunner) Teardown(ctx context.Context, containers *IcebergContainer) error
```

To start the containers do as follows

```go
icebergRunner := kit.IcebergRunner{}
var containers *kit.IcebergContainer
containers, err := icebergRunner.Setup(ctx)
if err != nil {
    panic(err)
}

defer func() {
    if err := icebergRunner.Teardown(ctx, containers); err != nil {
        panic(err)
    }
}()
```
IcebergContainer is a struct that contains the reference to all involved containers and connection to trino db

```go
type IcebergContainer struct {
	Trino       testcontainers.Container
	Db          *sql.DB
	Postgres    testcontainers.Container
	Minio       testcontainers.Container
	MinioServer testcontainers.Container
	RestIceberg testcontainers.Container
	Network     testcontainers.Network
}
```

Is it also possible to specify trino and postgres versions using this method

```go
containers, err = icebergRunner.SetupWithCustomVersions(ctx, "455", "14")
if err != nil {
    panic(err)
}
```

### Default versions of images
* trino: 466
* postgres: 15
