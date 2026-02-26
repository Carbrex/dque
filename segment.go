package dque

//
// Copyright (c) 2018 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

//
// This is a segment of a memory-efficient FIFO durable queue.  Items in the queue must be of the same type.
//
// Each qSegment instance corresponds to a file on disk.
//
// This segment is both persistent and in-memory so there is a memory limit to the size
// (which is why it is just a segment instead of being used for the entire queue).
//

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
)

// ErrCorruptedSegment is returned when a segment file cannot be opened due to inconsistent formatting.
// Recovery may be possible by clearing or deleting the file, then reloading using dque.New().
type ErrCorruptedSegment struct {
	Path string
	Err  error
}

// Error returns a string describing ErrCorruptedSegment
func (e ErrCorruptedSegment) Error() string {
	return fmt.Sprintf("segment file %s is corrupted: %s", e.Path, e.Err)
}

// Unwrap returns the wrapped error
func (e ErrCorruptedSegment) Unwrap() error {
	return e.Err
}

// ErrUnableToDecode is returned when an object cannot be decoded.
type ErrUnableToDecode struct {
	Path string
	Err  error
}

// Error returns a string describing ErrUnableToDecode error
func (e ErrUnableToDecode) Error() string {
	return fmt.Sprintf("object in segment file %s cannot be decoded: %s", e.Path, e.Err)
}

// Unwrap returns the wrapped error
func (e ErrUnableToDecode) Unwrap() error {
	return e.Err
}

var (
	errEmptySegment = errors.New("Segment is empty")
)

// segWritePool holds reusable byte slice pointers for the add() write path.
// Each entry is *[]byte so the pool can grow the backing array on reallocations.
var segWritePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 1024)
		return &buf
	},
}

// qSegment represents a portion (segment) of a persistent queue
type qSegment struct {
	dirPath       string
	number        int
	objects       []interface{}
	objectBuilder func() interface{}
	file          *os.File
	mutex         sync.Mutex
	removeCount   int
	turbo         bool
	maybeDirty    bool  // filesystem changes may not have been flushed to disk
	syncCount     int64 // for testing
	codec         Codec
}

// load reads all objects from the queue file into a slice.
// Returns ErrCorruptedSegment or ErrUnableToDecode for errors pertaining to file contents.
func (seg *qSegment) load() error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	// Open the file in read mode
	f, err := os.OpenFile(seg.filePath(), os.O_RDONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "error opening file: "+seg.filePath())
	}
	defer f.Close()
	seg.file = f

	// Loop until we can load no more
	for {
		// Read the 4-byte length prefix
		lenBytes := make([]byte, 4)
		if n, err := io.ReadFull(seg.file, lenBytes); err != nil {
			if err == io.EOF {
				return nil
			}
			return ErrCorruptedSegment{
				Path: seg.filePath(),
				Err:  errors.Wrapf(err, "error reading object length (read %d/4 bytes)", n),
			}
		}

		// Convert the bytes into a 32-bit unsigned int
		payloadLen := binary.LittleEndian.Uint32(lenBytes)
		if payloadLen == 0 {
			// A zero-length prefix is a tombstone marking a consumed item.
			if len(seg.objects) == 0 {
				return ErrCorruptedSegment{
					Path: seg.filePath(),
					Err:  fmt.Errorf("excess deletion records (%d)", seg.removeCount+1),
				}
			}
			seg.objects = seg.objects[1:]
			seg.removeCount++
			continue
		}

		data := make([]byte, int(payloadLen))
		if _, err := io.ReadFull(seg.file, data); err != nil {
			return ErrCorruptedSegment{
				Path: seg.filePath(),
				Err:  errors.Wrap(err, "error reading payload from file"),
			}
		}

		// Decode the bytes into an object using the configured codec
		object, decErr := seg.codec.Decode(data, seg.objectBuilder)
		if decErr != nil {
			return ErrUnableToDecode{
				Path: seg.filePath(),
				Err:  errors.Wrapf(decErr, "failed to decode object"),
			}
		}

		seg.objects = append(seg.objects, object)
	}
}

// peek returns the first item in the segment without removing it.
// If the queue is already empty, the emptySegment error will be returned.
func (seg *qSegment) peek() (interface{}, error) {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if len(seg.objects) == 0 {
		return nil, errEmptySegment
	}

	return seg.objects[0], nil
}

// remove removes and returns the first item in the segment and appends
// a zero-length marker to the queue file to record the removal.
// If the queue is already empty, the emptySegment error will be returned.
func (seg *qSegment) remove() (interface{}, error) {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if len(seg.objects) == 0 {
		return nil, errEmptySegment
	}

	// A 4-byte zero is the tombstone that records a removal without rewriting the file.
	deleteLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(deleteLenBytes, 0)

	if _, err := seg.file.Write(deleteLenBytes); err != nil {
		return nil, errors.Wrapf(err, "failed to remove item from segment %d", seg.number)
	}

	object := seg.objects[0]
	seg.objects = seg.objects[1:]
	seg.removeCount++

	if err := seg._sync(); err != nil {
		return nil, err
	}

	return object, nil
}

// add adds an item to the in-memory queue segment and appends it to the persistent file.
// The 4-byte length prefix and encoded payload are written in a single syscall using a
// pooled buffer to avoid a per-call heap allocation.
func (seg *qSegment) add(object interface{}) error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	// Grab a reusable buffer. Reserve the first 4 bytes as a length placeholder so
	// the codec can append the encoded bytes after it with no extra copy.
	bp := segWritePool.Get().(*[]byte)
	*bp = (*bp)[:0]
	*bp = append(*bp, 0, 0, 0, 0) // length placeholder

	result, encErr := seg.codec.Encode(*bp, object)
	if encErr != nil {
		segWritePool.Put(bp)
		return errors.Wrap(encErr, "error encoding object")
	}
	// result = [placeholder(4)] + [encoded payload]
	payloadLen := len(result) - 4
	binary.LittleEndian.PutUint32(result[:4], uint32(payloadLen))

	// Single syscall: length prefix and payload combined.
	_, writeErr := seg.file.Write(result)

	// Return the (possibly grown) backing array before checking the error.
	*bp = result[:0]
	segWritePool.Put(bp)

	if writeErr != nil {
		return errors.Wrapf(writeErr, "failed to write object to segment %d", seg.number)
	}

	seg.objects = append(seg.objects, object)

	return seg._sync()
}

// size returns the number of objects in this segment.
// The size does not include items that have been removed.
func (seg *qSegment) size() int {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	return len(seg.objects)
}

// sizeOnDisk returns the number of objects in memory plus removed objects. This
// number will match the number of objects still on disk.
// This number is used to keep the file from growing forever when items are
// removed about as fast as they are added.
func (seg *qSegment) sizeOnDisk() int {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	return len(seg.objects) + seg.removeCount
}

// delete wipes out the queue and its persistent state
func (seg *qSegment) delete() error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if err := seg.file.Close(); err != nil {
		return errors.Wrap(err, "unable to close the segment file before deleting")
	}

	if err := os.Remove(seg.filePath()); err != nil {
		return errors.Wrap(err, "error deleting file: "+seg.filePath())
	}

	seg.objects = seg.objects[:0]
	seg.file = nil

	return nil
}

func (seg *qSegment) fileName() string {
	return fmt.Sprintf("%013d.dque", seg.number)
}

func (seg *qSegment) filePath() string {
	return path.Join(seg.dirPath, seg.fileName())
}

// turboOn allows the filesystem to decide when to sync file changes to disk.
// Speed is greatly increased by turning turbo on, however there is some
// risk of losing data should a power-loss occur.
func (seg *qSegment) turboOn() {
	seg.turbo = true
}

// turboOff re-enables the "safety" mode that syncs every file change to disk as
// they happen.
func (seg *qSegment) turboOff() error {
	if !seg.turbo {
		// turboOff is known to be called twice when the first and last segments
		// are the same.
		return nil
	}
	if err := seg.turboSync(); err != nil {
		return err
	}
	seg.turbo = false
	return nil
}

// turboSync does an fsync to disk if turbo is on.
func (seg *qSegment) turboSync() error {
	if !seg.turbo {
		return nil
	}
	if seg.maybeDirty {
		if err := seg.file.Sync(); err != nil {
			return errors.Wrap(err, "unable to sync file changes.")
		}
		seg.syncCount++
		seg.maybeDirty = false
	}
	return nil
}

// _sync must only be called by the add and remove methods on qSegment.
// Only syncs if turbo is off.
func (seg *qSegment) _sync() error {
	if seg.turbo {
		seg.maybeDirty = true
		return nil
	}

	if err := seg.file.Sync(); err != nil {
		return errors.Wrap(err, "unable to sync file changes in _sync method.")
	}
	seg.syncCount++
	seg.maybeDirty = false
	return nil
}

// close is used when this is the last segment, but is now full, so we are
// creating a new last segment.
// This should only be called if this segment is not also the first segment.
func (seg *qSegment) close() error {
	if err := seg.file.Close(); err != nil {
		return errors.Wrapf(err, "unable to close segment file %s.", seg.fileName())
	}
	return nil
}

// newQueueSegment creates a new, persistent segment of the queue.
func newQueueSegment(dirPath string, number int, turbo bool, builder func() interface{}, codec Codec) (*qSegment, error) {

	seg := qSegment{
		dirPath:       dirPath,
		number:        number,
		turbo:         turbo,
		objectBuilder: builder,
		codec:         codec,
	}

	if !dirExists(seg.dirPath) {
		return nil, errors.New("dirPath is not a valid directory: " + seg.dirPath)
	}

	if fileExists(seg.filePath()) {
		return nil, errors.New("file already exists: " + seg.filePath())
	}

	var err error
	seg.file, err = os.OpenFile(seg.filePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating file: %s.", seg.filePath())
	}

	return &seg, nil
}

// openQueueSegment reads an existing persistent segment of the queue into memory.
func openQueueSegment(dirPath string, number int, turbo bool, builder func() interface{}, codec Codec) (*qSegment, error) {

	seg := qSegment{
		dirPath:       dirPath,
		number:        number,
		turbo:         turbo,
		objectBuilder: builder,
		codec:         codec,
	}

	if !dirExists(seg.dirPath) {
		return nil, errors.New("dirPath is not a valid directory: " + seg.dirPath)
	}

	if !fileExists(seg.filePath()) {
		return nil, errors.New("file does not exist: " + seg.filePath())
	}

	if err := seg.load(); err != nil {
		return nil, errors.Wrap(err, "unable to load queue segment in "+dirPath)
	}

	var err error
	seg.file, err = os.OpenFile(seg.filePath(), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "error opening file: "+seg.filePath())
	}

	return &seg, nil
}
