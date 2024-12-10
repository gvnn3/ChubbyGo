module ChubbyGo

go 1.22

require (
    github.com/sony/sonyflake v1.2.0
    github.com/OneOfOne/xxhash v1.2.8
    github.com/redis/go-redis/v9 v9.5.1
)

replace ChubbyGo/Flake => ./Flake
