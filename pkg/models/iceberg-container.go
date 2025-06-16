package models

import (
	"database/sql"
	"github.com/testcontainers/testcontainers-go"
)

type IcebergContainer struct {
	Trino       testcontainers.Container
	Db          *sql.DB
	Postgres    testcontainers.Container
	Minio       testcontainers.Container
	MinioServer testcontainers.Container
	RestIceberg testcontainers.Container
}
