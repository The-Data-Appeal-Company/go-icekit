package kit

import (
	"database/sql"

	"github.com/testcontainers/testcontainers-go"
)

type IcebergContainer struct {
	Trino       testcontainers.Container
	Db          *sql.DB
	Postgres    testcontainers.Container
	ObjectStore testcontainers.Container
	RestIceberg testcontainers.Container
	Network     testcontainers.Network
}
