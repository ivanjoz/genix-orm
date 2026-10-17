module github.com/ivanjoz/genix-orm/search/daemon

go 1.27

require github.com/ivanjoz/genix-orm/db v0.0.0

require (
	github.com/ivanjoz/genix-orm/utils v0.0.0 // indirect
	github.com/viant/xunsafe v0.11.0 // indirect
)

replace github.com/ivanjoz/genix-orm/db => ../../db

replace github.com/ivanjoz/genix-orm/utils => ../../utils
