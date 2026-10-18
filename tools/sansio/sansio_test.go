package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), Analyzer,
		"github.com/nvcnvn/rock-solid/internal/core",
		"github.com/nvcnvn/rock-solid/internal/core/sub",
		"github.com/nvcnvn/rock-solid/internal/loop",
		"github.com/nvcnvn/rock-solid/internal/driver/uring",
		"github.com/nvcnvn/rock-solid/internal/csi",
		"github.com/nvcnvn/rock-solid/cmd/rockd",
	)
}

func TestClassify(t *testing.T) {
	cases := map[string]packageKind{
		"github.com/nvcnvn/rock-solid/internal/core":                                                   sansIO,
		"github.com/nvcnvn/rock-solid/internal/core [github.com/nvcnvn/rock-solid/internal/core.test]": sansIO,
		"github.com/nvcnvn/rock-solid/internal/sim/disk":                                               sansIO,
		"github.com/nvcnvn/rock-solid/internal/loop":                                                   dataPath,
		"github.com/nvcnvn/rock-solid/internal/loopback":                                               sansIO,
		"github.com/nvcnvn/rock-solid/internal/driver/uring":                                           dataPath,
		"github.com/nvcnvn/rock-solid/internal/frontend/fuse":                                          dataPath,
		"github.com/nvcnvn/rock-solid/internal/core.test":                                              unchecked,
		"github.com/nvcnvn/rock-solid/internal/csi":                                                    unchecked,
		"github.com/nvcnvn/rock-solid/internal/csi/grpc":                                               unchecked,
		"github.com/nvcnvn/rock-solid/cmd/rockd":                                                       unchecked,
		"github.com/nvcnvn/rock-solid/tools/sansio":                                                    unchecked,
		"github.com/other/internal/core":                                                               unchecked,
	}
	for path, want := range cases {
		if got := classify(path); got != want {
			t.Errorf("classify(%q) = %v, want %v", path, got, want)
		}
	}
}
