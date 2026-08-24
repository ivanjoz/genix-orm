module github.com/ivanjoz/genix-orm

go 1.26

toolchain go1.27.0

require (
	github.com/fatih/color v1.19.0
	github.com/ivanjoz/colbin v0.0.0-20260720041505-3f9040cb9613
	github.com/kr/pretty v0.1.0
	github.com/viant/xunsafe v0.11.0
	golang.org/x/sync v0.22.0
)

require (
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.42.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

require (
	github.com/gocql/gocql v1.6.0
	github.com/ivanjoz/genix-orm/db v0.0.0
)

replace github.com/gocql/gocql v1.6.0 => github.com/scylladb/gocql v1.13.0

replace github.com/ivanjoz/genix-orm/db => ./db
