package config

import (
	"database/sql"
	"fmt"
	"strings"
)

type TrinoConf struct {
	User          string
	Host          string
	Schema        string
	Catalog       string
	Port          int
	SessionParams map[string]string
}

func (t TrinoConf) ConnectionString() string {
	params := make([]string, len(t.SessionParams))
	i := 0
	for key, value := range t.SessionParams {
		params[i] = fmt.Sprintf("%s=%s", key, value)
		i++
	}
	var sessionParamStr string
	if len(t.SessionParams) > 0 {
		sessionParamStr = fmt.Sprintf("&session_properties=%s", strings.Join(params, ","))
	}
	return fmt.Sprintf("http://%s@%s:%d?catalog=%s&schema=%s%s",
		t.User,
		t.Host,
		t.Port,
		t.Catalog,
		t.Schema,
		sessionParamStr)
}

func (t TrinoConf) CreateDB() (*sql.DB, error) {
	return sql.Open("trino", t.ConnectionString())
}
