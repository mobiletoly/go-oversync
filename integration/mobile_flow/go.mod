module github.com/mobiletoly/go-oversync/integration/mobile_flow

go 1.25.0

toolchain go1.25.12

replace github.com/mobiletoly/go-oversync => ../..

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.9.1
	github.com/lib/pq v1.12.0
	github.com/mattn/go-sqlite3 v1.14.37
	github.com/mobiletoly/go-oversync v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/cyberphone/json-canonicalization v0.0.0-20241213102144-19d51d7fe467 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
