module github.com/nvcnvn/rock-solid/spikes/lsm-study-reads

go 1.27

require (
	github.com/cockroachdb/pebble v0.0.0
	github.com/nvcnvn/rock-solid v0.0.0
	github.com/zeebo/xxh3 v1.1.0
)

require (
	github.com/FastFilter/xorfilter v0.4.2-0.20260120015552-4e5a4d9df65a // indirect
	github.com/cockroachdb/crlib v0.0.0-20251122031428-fe658a2dbda1 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.31.0 // indirect
)

replace (
	github.com/cockroachdb/pebble => ./work/pebble
	github.com/nvcnvn/rock-solid => ../..
)
