// Package db implements persistence for tracked companies against
// PostgreSQL. Postgres was chosen (over MongoDB/Cassandra) because company
// records are a fixed, relational schema with the need for fast exact-match
// lookups by symbol -- a classic relational access pattern that doesn't
// benefit from a schemaless document store or a wide-column store built
// for massive write throughput.
package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("company not found")

// Company is the internal domain representation of a tracked TSX company.
// The gRPC layer maps this to/from the generated protobuf message so that
// the persistence layer has no dependency on generated code.
type Company struct {
	Symbol      string
	Name        string
	Exchange    string
	Currency    string
	LastUpdated time.Time
}

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(ctx context.Context, dsn string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	r.pool.Close()
}

// Migrate applies the schema in migrationsSQL (typically read from the
// migrations/ directory at process startup). It's a minimal idempotent
// migrator suitable for this service; use a dedicated tool (golang-migrate,
// atlas, etc.) if the schema grows more complex.
func (r *Repository) Migrate(ctx context.Context, migrationSQL string) error {
	_, err := r.pool.Exec(ctx, migrationSQL)
	if err != nil {
		return fmt.Errorf("run migration: %w", err)
	}
	return nil
}

// UpsertCompanies inserts or updates a batch of companies in a single
// transaction, refreshing last_updated to now().
func (r *Repository) UpsertCompanies(ctx context.Context, companies []Company) error {
	if len(companies) == 0 {
		return nil
	}

	const colsPerRow = 4
	args := make([]any, 0, len(companies)*colsPerRow)
	var b strings.Builder
	b.WriteString(`INSERT INTO companies (symbol, name, exchange, currency, last_updated) VALUES `)

	for i, c := range companies {
		if i > 0 {
			b.WriteByte(',')
		}
		n := i * colsPerRow
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d, now())", n+1, n+2, n+3, n+4)
		args = append(args, c.Symbol, c.Name, c.Exchange, c.Currency)
	}

	b.WriteString(` ON CONFLICT (symbol) DO UPDATE SET
		name = EXCLUDED.name,
		exchange = EXCLUDED.exchange,
		currency = EXCLUDED.currency,
		last_updated = now()
	`)

	if _, err := r.pool.Exec(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("upsert companies: %w", err)
	}
	return nil
}

// InsertSymbolStubs ensures a row exists (with just symbol/name/exchange)
// for every given symbol, without touching existing rows. This lets the
// refresher discover "new to us" symbols from the cheap symbol-list
// endpoint immediately, and backfill full profile data over subsequent
// refresh cycles.
func (r *Repository) InsertSymbolStubs(ctx context.Context, stubs []Company) error {
	if len(stubs) == 0 {
		return nil
	}

	const colsPerRow = 4
	args := make([]any, 0, len(stubs)*colsPerRow)
	var b strings.Builder
	b.WriteString(`INSERT INTO companies (symbol, name, exchange, currency, last_updated) VALUES `)

	for i, s := range stubs {
		if i > 0 {
			b.WriteByte(',')
		}
		n := i * colsPerRow
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d, TIMESTAMPTZ '1970-01-01')", n+1, n+2, n+3, n+4)
		args = append(args, s.Symbol, s.Name, s.Exchange, s.Currency)
	}
	b.WriteString(` ON CONFLICT (symbol) DO NOTHING`)

	if _, err := r.pool.Exec(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("insert stubs: %w", err)
	}
	return nil
}

// TrackedCount returns how many companies currently exist in the table.
func (r *Repository) TrackedCount(ctx context.Context) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM companies`).Scan(&n)
	return n, err
}

// DeleteBySymbols removes companies whose symbols are in the given slice.
// Returns the number of rows deleted.
func (r *Repository) DeleteBySymbols(ctx context.Context, symbols []string) (int64, error) {
	if len(symbols) == 0 {
		return 0, nil
	}
	n, err := r.pool.Exec(ctx, `DELETE FROM companies WHERE symbol = ANY($1)`, symbols)
	if err != nil {
		return 0, fmt.Errorf("delete by symbols: %w", err)
	}
	return n.RowsAffected(), nil
}

// AllSymbols returns every tracked symbol in the database.
func (r *Repository) AllSymbols(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT symbol FROM companies`)
	if err != nil {
		return nil, fmt.Errorf("query all symbols: %w", err)
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		symbols = append(symbols, s)
	}
	return symbols, rows.Err()
}

// GetBySymbol fetches a single company. Symbol match is case-insensitive.
func (r *Repository) GetBySymbol(ctx context.Context, symbol string) (*Company, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT symbol, name, exchange, currency, last_updated
		FROM companies WHERE upper(symbol) = upper($1)
	`, symbol)

	c, err := scanCompanyRow(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return c, nil
}

// List returns a page of companies ordered by symbol, using keyset
// pagination on symbol (afterSymbol is exclusive).
func (r *Repository) List(ctx context.Context, afterSymbol string, pageSize int) ([]Company, error) {
	var args []any
	query := `SELECT symbol, name, exchange, currency, last_updated FROM companies`

	if afterSymbol != "" {
		query += ` WHERE symbol > $1`
		args = append(args, afterSymbol)
	}
	query += fmt.Sprintf(` ORDER BY symbol ASC LIMIT $%d`, len(args)+1)
	args = append(args, pageSize)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list companies: %w", err)
	}
	defer rows.Close()

	var out []Company
	for rows.Next() {
		c, err := scanCompanyRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// Count returns the total number of companies in the table.
func (r *Repository) Count(ctx context.Context) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM companies`).Scan(&n)
	return n, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCompanyRow(row rowScanner) (*Company, error) {
	var c Company
	err := row.Scan(&c.Symbol, &c.Name, &c.Exchange, &c.Currency, &c.LastUpdated)
	if err != nil {
		return nil, err
	}
	return &c, nil
}
