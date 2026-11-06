module github.com/ivanjoz/genix-orm/conformance

go 1.27

toolchain go1.27.0

require (
	github.com/ivanjoz/colbin v0.9.2
	github.com/ivanjoz/genix-orm v0.0.0
	github.com/ivanjoz/genix-orm/db v0.0.0
	github.com/ivanjoz/genix-orm/utils v0.0.0
	github.com/ivanjoz/genix-orm/ydbdriver v0.0.0
)

require (
	github.com/fatih/color v1.19.0 // indirect
	github.com/gocql/gocql v1.6.0 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/ivanjoz/genix-orm/dataframe v0.0.0 // indirect
	github.com/ivanjoz/genix-orm/fdb v0.0.0 // indirect
	github.com/jonboulle/clockwork v0.5.0 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/viant/xunsafe v0.11.0 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20260810122915-65bfd5c4b705 // indirect
	github.com/ydb-platform/ydb-go-sdk/v3 v3.156.1 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/ivanjoz/genix-orm => ../

replace github.com/ivanjoz/genix-orm/db => ../db

replace github.com/ivanjoz/genix-orm/utils => ../utils

replace github.com/ivanjoz/genix-orm/dataframe => ../dataframe

replace github.com/ivanjoz/genix-orm/fdb => ../fdb

replace github.com/ivanjoz/genix-orm/ydbdriver => ../ydbdriver

// Same fork the root module builds against; see ../thirdparty/README.md.
replace github.com/gocql/gocql v1.6.0 => ../thirdparty/gocql
