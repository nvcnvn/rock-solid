package core

import (
	"encoding/binary"
	"hash/crc32"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC32C checksums b with the Castagnoli polynomial, as every disk block and header is.
func CRC32C(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// CRC32CUpdate continues a checksum over more bytes.
func CRC32CUpdate(crc uint32, b []byte) uint32 { return crc32.Update(crc, castagnoli, b) }

// CRC32CSize is the stored width of a checksum.
const CRC32CSize = 4

// PutCRC32C stores a checksum little-endian in the first four bytes of b.
func PutCRC32C(b []byte, crc uint32) { binary.LittleEndian.PutUint32(b, crc) }

// GetCRC32C reads a checksum stored by PutCRC32C.
func GetCRC32C(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }

// AppendCRC32C appends the checksum of data to b.
func AppendCRC32C(b, data []byte) []byte { return binary.LittleEndian.AppendUint32(b, CRC32C(data)) }
