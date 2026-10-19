// Package subblock models how sub-block writes become whole 4 KiB blocks (merge_test.go), what
// small fsync'd appends cost in the disk log (appends_test.go), and what a small file costs
// (space_test.go). See README.md.
package subblock
