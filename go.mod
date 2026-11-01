module github.com/nvcnvn/rock-solid

go 1.27

toolchain go1.27.0

require (
	github.com/anishathalye/porcupine v1.3.1
	github.com/cockroachdb/datadriven v1.0.2
	github.com/zeebo/xxh3 v1.1.0
	go.etcd.io/raft/v3 v3.6.0
	golang.org/x/tools v0.52.0
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)

replace go.etcd.io/raft/v3 => ./third_party/raft
