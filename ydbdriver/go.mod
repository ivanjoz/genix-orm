module github.com/ivanjoz/genix-orm/ydbdriver

go 1.27

toolchain go1.27.0

require (
	github.com/ivanjoz/colbin v0.9.2
	github.com/ivanjoz/genix-orm/dataframe v0.0.0
	github.com/ivanjoz/genix-orm/db v0.0.0
	github.com/ydb-platform/ydb-go-sdk/v3 v3.156.1
)

require (
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/ivanjoz/genix-orm/utils v0.0.0 // indirect
	github.com/jonboulle/clockwork v0.5.0 // indirect
	github.com/viant/xunsafe v0.11.0 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20260810122915-65bfd5c4b705 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace github.com/ivanjoz/genix-orm/dataframe => ../dataframe

replace github.com/ivanjoz/genix-orm/db => ../db

replace github.com/ivanjoz/genix-orm/utils => ../utils
