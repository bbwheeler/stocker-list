package db

import _ "embed"

//go:embed migrations/0001_init.sql
var initSchemaSQL string

// InitSchemaSQL returns the embedded schema-creation SQL, run once at
// service startup (see Repository.Migrate). Keeping it embedded means the
// binary is self-contained and doesn't need the migrations/ directory
// present at runtime (e.g. in the Docker image).
func InitSchemaSQL() string {
	return initSchemaSQL
}
