module github.com/ivanjoz/genix-orm/conformance

go 1.27

toolchain go1.27.0

require (
	github.com/ivanjoz/colbin v0.9.2
	github.com/ivanjoz/genix-orm v0.0.0
	github.com/ivanjoz/genix-orm/db v0.0.0
	github.com/ivanjoz/genix-orm/mariadbdriver v0.0.0
	github.com/ivanjoz/genix-orm/obdriver v0.0.0
	github.com/ivanjoz/genix-orm/pgdriver v0.0.0
	github.com/ivanjoz/genix-orm/utils v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/gocql/gocql v1.6.0 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/ivanjoz/genix-orm/dataframe v0.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.12.0 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rogpeppe/go-internal v1.6.1 // indirect
	github.com/viant/xunsafe v0.11.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/ivanjoz/genix-orm => ../

replace github.com/ivanjoz/genix-orm/db => ../db

replace github.com/ivanjoz/genix-orm/utils => ../utils

replace github.com/ivanjoz/genix-orm/dataframe => ../dataframe

// Same fork the root module builds against; see ../thirdparty/README.md.
replace github.com/gocql/gocql v1.6.0 => ../thirdparty/gocql

replace github.com/ivanjoz/genix-orm/obdriver => ../obdriver

replace github.com/ivanjoz/genix-orm/pgdriver => ../pgdriver

replace github.com/ivanjoz/genix-orm/mariadbdriver => ../mariadbdriver
