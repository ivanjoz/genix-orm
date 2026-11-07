module github.com/ivanjoz/genix-orm/mariadbdriver

go 1.27

toolchain go1.27.0

require (
	github.com/go-sql-driver/mysql v1.10.1
	github.com/ivanjoz/genix-orm/dataframe v0.0.0
	github.com/ivanjoz/genix-orm/db v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/ivanjoz/colbin v0.9.2 // indirect
	github.com/ivanjoz/genix-orm/utils v0.0.0 // indirect
	github.com/viant/xunsafe v0.11.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
)

replace github.com/ivanjoz/genix-orm/dataframe => ../dataframe

replace github.com/ivanjoz/genix-orm/db => ../db

replace github.com/ivanjoz/genix-orm/utils => ../utils
