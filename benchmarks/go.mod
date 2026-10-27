module github.com/ivanjoz/genix-orm/benchmarks

go 1.27

toolchain go1.27.0

require (
	github.com/ivanjoz/genix-orm/dataframe v0.0.0
	github.com/ivanjoz/genix-orm/utils v0.0.0
)

// orm is a symlink to the genix-orm checkout under test (link.sh): this branch's own code by default,
// the main checkout while iterating, a worktree of an older commit in an A/B (ab.sh).
replace github.com/ivanjoz/genix-orm/dataframe => ./orm/dataframe

replace github.com/ivanjoz/genix-orm/utils => ./orm/utils
