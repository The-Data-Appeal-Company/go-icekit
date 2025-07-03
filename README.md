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

To start the containers do as follows

```go
icebergRunner := kit.IcebergRunner{}
var containers *kit.IcebergContainer
containers = icebergRunner.Setup(ctx)
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
}
```

There is also teardown method to terminate the containers and close trino connection

```go
defer icebergRunner.Teardown(ctx, containers)
```

Is it also possible to specify trino and postgres versions using this method

```go
containers = icebergRunner.SetupWithCustomVersions(ctx, "455", "14")
```

### Default versions of images
* trino: 466
* postgres: 15