module ChubbyGo

go 1.22

require (
	github.com/OneOfOne/xxhash v1.2.8
	github.com/redis/go-redis/v9 v9.5.1
	github.com/sony/sonyflake v1.2.0
)

require github.com/cespare/xxhash/v2 v2.3.0 // indirect

replace ChubbyGo/Flake => ./Flake
