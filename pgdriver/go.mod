module github.com/ivanjoz/genix-orm/pgdriver

go 1.27

toolchain go1.27.0

require (
	github.com/ivanjoz/colbin v0.11.0
	github.com/ivanjoz/genix-orm/dataframe v0.0.0
	github.com/ivanjoz/genix-orm/db v0.0.0
	github.com/ivanjoz/genix-orm/utils v0.0.0
	github.com/jackc/pgx/v5 v5.12.0
	golang.org/x/sync v0.22.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/viant/xunsafe v0.11.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/ivanjoz/genix-orm/dataframe => ../dataframe

replace github.com/ivanjoz/genix-orm/db => ../db

replace github.com/ivanjoz/genix-orm/utils => ../utils
