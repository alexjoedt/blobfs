package blobfs

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// ID represents a 12-byte unique identifier similar to MongoDB's ObjectID.
type ID [12]byte

var (
	// machineID is a 3-byte unique identifier for this machine
	machineID = readMachineID()

	// counter is an atomically incremented counter (3 bytes)
	counter = readRandomUint32()
)

// readMachineID generates a 3-byte machine identifier.
func readMachineID() [3]byte {
	var mid [3]byte
	hostname, err := os.Hostname()
	if err != nil {
		// If we can't get hostname, use random bytes
		_, _ = io.ReadFull(rand.Reader, mid[:])
		return mid
	}

	// Use hostname hash for machine ID
	hw := make([]byte, 32)
	copy(hw, hostname)
	copy(mid[:], hw[:3])
	return mid
}

// readRandomUint32 generates a random uint32 for counter initialization
func readRandomUint32() uint32 {
	var b [4]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	return binary.BigEndian.Uint32(b[:])
}

// newID generates a new unique 12-byte ID.
// Layout:
//
//   - 4 bytes: timestamp (seconds since epoch)
//
//   - 3 bytes: machine identifier
//
//   - 2 bytes: process id
//
//   - 3 bytes: counter
//
//     Provides uniqueness across time, machines, processes,
//
// and multiple IDs within the same second. Similar to MongoDB ObjectID.
//
// Produces URL-safe, human-readable identifiers.
func newID() string {
	var id ID

	// Timestamp (4 bytes)
	timestamp := uint32(time.Now().Unix())
	binary.BigEndian.PutUint32(id[0:4], timestamp)

	// Machine ID (3 bytes)
	copy(id[4:7], machineID[:])

	// Process ID (2 bytes)
	pid := uint16(os.Getpid())
	binary.BigEndian.PutUint16(id[7:9], pid)

	// Counter (3 bytes) - atomically incremented
	c := atomic.AddUint32(&counter, 1)
	id[9] = byte(c >> 16)
	id[10] = byte(c >> 8)
	id[11] = byte(c)

	return hex.EncodeToString(id[:])
}
