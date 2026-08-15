package store

import (
	"encoding/binary"
	"fmt"
)

// TerminalKey returns the bucket key for a terminal-id / sequence pair. The
// sequence is encoded as an 8-byte big-endian integer so that lexicographic
// order matches numeric order within a single terminal id.
func TerminalKey(terminalID string, seq int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seq))
	return []byte(fmt.Sprintf("%s\x00%s", terminalID, string(buf[:])))
}

// ParseTerminalKey reverses TerminalKey.
func ParseTerminalKey(k []byte) (terminalID string, seq int64, ok bool) {
	for i, c := range k {
		if c == 0 {
			terminalID = string(k[:i])
			if i+9 > len(k) {
				return "", 0, false
			}
			seq = int64(binary.BigEndian.Uint64(k[i+1 : i+9]))
			return terminalID, seq, true
		}
	}
	return "", 0, false
}
