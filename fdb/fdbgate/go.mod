module github.com/ivanjoz/genix-orm/fdb/fdbgate

go 1.27

toolchain go1.27.0

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20260827171757-946e01da3a60
	github.com/ivanjoz/genix-orm/fdb v0.0.0
)

require github.com/ivanjoz/colbin v0.9.2 // indirect

replace github.com/ivanjoz/genix-orm/fdb => ../
