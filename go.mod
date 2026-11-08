module github.com/ivanjoz/genix-orm

go 1.27

toolchain go1.27.0

require github.com/viant/xunsafe v0.11.0 // indirect

require (
	github.com/ivanjoz/colbin v0.9.2
	github.com/ivanjoz/genix-orm/dataframe v0.0.0
	github.com/ivanjoz/genix-orm/db v0.0.0
	github.com/ivanjoz/genix-orm/fdb v0.0.0
	github.com/ivanjoz/genix-orm/utils v0.0.0
	golang.org/x/sync v0.22.0
)

replace github.com/ivanjoz/genix-orm/db => ./db

replace github.com/ivanjoz/genix-orm/utils => ./utils

replace github.com/ivanjoz/genix-orm/dataframe => ./dataframe

replace github.com/ivanjoz/genix-orm/fdb => ./fdb
