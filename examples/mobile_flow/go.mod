module github.com/mobiletoly/go-oversync/examples/mobile_flow

go 1.25.0

replace github.com/mobiletoly/go-oversync => ../..

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.8.0
	github.com/lib/pq v1.12.0
	github.com/mattn/go-sqlite3 v1.14.37
	github.com/mobiletoly/go-oversync v0.0.0-00010101000000-000000000000
)

require (
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.35.0 // indirect
)
