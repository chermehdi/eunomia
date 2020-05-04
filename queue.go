package eunomia

import (
	"bytes"
	"errors"
	"io"
	"os"
)

var buffer = make([]byte, 64)

var (
	NewFileError                        = errors.New("file has never been initialized")
	CorruptVersionError                 = errors.New("invalid version in the file header")
	UnexpectedNumberOfWrittenBytesError = errors.New("the number of written bytes and the number of expected bytes to be written is different")
	EmptyQueueError                     = errors.New("cannot peek or poll from an empty queue")
)

// Magic number to act as the version, for backward compatibility guarantees.
const MagicVersionNumber int32 = 0x23

type Queue interface {
	Push(element interface{}) error

	Poll() (interface{}, error)

	Peek() (interface{}, error)

	Size() int64

	Delete() error
}

// Types that we can push onto the queue should adhere to the following contract:
//   Write: An element should be responsible of writing itself as a sequence of bytes.
//    (Note: This implies a temporary buffer is allocated this should be taken care of in the next iteration)
//
//   Read: An element should be able to restore it's state from a given io.Reader
type Serializer interface {
	Write(interface{}) []byte

	Read(reader io.Reader) interface{}
}

// A Flat file-based implementation of the Queue interface.
// TODO(chermehdi): add docs and examples.
type FileQueue struct {
	filePath   string
	writer     *QueueProtocolWriter
	serializer Serializer
}

// Creates or restores a new flat-file queue from the given file path.
// If the file is corrupt (i.e it already exists and it has an unexpected format) this will return a corruption error.
func NewFileQueue(filePath string, serializer Serializer) (Queue, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0755) // maybe parametrize the default permissions
	if err != nil {
		return nil, err
	}
	protoWriter, err := NewQueueWriter(file)
	if err != nil {
		return nil, err
	}
	return &FileQueue{
		filePath:   filePath,
		writer:     protoWriter,
		serializer: serializer,
	}, nil
}

// There two cases when pushing to the queue
// 1- The queue is empty, this is the first element the head and tail are pointing to the same offset
//    And this will stay the after the call to push, we only are going to update the lengths
// 2- The queue already contains some 1 or more elements.
func (f *FileQueue) Push(element interface{}) error {
	data := f.serializer.Write(element)
	header := f.writer.header
	dataLength := int64(len(data))
	if f.Size() == 0 {
		header.tail.length = dataLength
		header.head.length = dataLength
	} else {
		header.tail.offset = header.tail.offset + header.tail.length + 8
		header.tail.length = dataLength
	}
	if err := WriteLong(f.writer.backingFile, header.tail.offset, dataLength); err != nil {
		return err
	}
	if _, err := WriteChunk(f.writer.backingFile, header.tail.offset+8, data); err != nil {
		return err
	}
	header.elementCount++
	if err := writeHeader(f.writer.backingFile, header); err != nil {
		return err
	}
	return nil
}

func (f *FileQueue) Poll() (interface{}, error) {
	panic("implement me")
}

func (f *FileQueue) Peek() (interface{}, error) {
	if f.Size() == 0 {
		return nil, EmptyQueueError
	}
	head := f.writer.header.head
	data, err := ReadChunk(f.writer.backingFile, head.offset+8, head.length)
	if err != nil {
		return nil, err
	}
	return f.serializer.Read(bytes.NewReader(data)), nil
}

func (f *FileQueue) Size() int64 {
	return f.writer.header.elementCount
}

func (f *FileQueue) Delete() error {
	return os.Remove(f.filePath)
}

// an element dictating how to write elements
// Protocol description:
//
// version                        4 byte
// created_at timestamp           8 bytes
// last_updated_at timestamp      8 bytes
// elementCount                   8 bytes
// firstElementLength             8 bytes
// firstElement                   firstElementLength bytes
// ..
// lastElementLength    8 bytes
// lastElement          lastElementLength bytes
type QueueProtocolWriter struct {
	backingFile *os.File
	header      *header
}

func (w *QueueProtocolWriter) updateTail(ptr *elementPtr) {
	if w.header.head.index == 0 {
		// First time
		w.header.head.length = ptr.length
	}
	w.header.tail = ptr
}

// Pointer to some data element in the file
// Each element is identified by it's start position and it's length (in bytes).
// The elements are written [elementLength,elementData] and the offset points to the first
// byte of the elementLength, i.e when reading any element, the data starts at offset + 8 and not at offset
type elementPtr struct {
	offset int64
	length int64
	index  int64
}

// The header structure of the queue file.
type header struct {
	elementCount int64
	version      int32
	flags        int32
	head         *elementPtr
	tail         *elementPtr
}

func NewQueueWriter(backingFile *os.File) (*QueueProtocolWriter, error) {
	writer := &QueueProtocolWriter{
		backingFile: backingFile,
	}
	if !fileExist(backingFile) {
		header, err := fillEmptyQueueFile(backingFile)
		if err != nil {
			return nil, err
		}
		writer.header = header
		return writer, nil
	}
	header, err := checkCorrupt(backingFile)
	if err != nil {
		return nil, err
	}
	writer.header = header
	return writer, nil
}

// Fills the passed empty header by the default header parameters and returns the created header.
// Any sort of error during the process is returned.
func fillEmptyQueueFile(file *os.File) (*header, error) {
	header := &header{
		version:      MagicVersionNumber,
		flags:        int32(0),
		elementCount: int64(0),
		head: &elementPtr{
			offset: int64(32),
			length: 0,
		},
		tail: &elementPtr{
			offset: int64(32),
			length: 0,
		},
	}
	err := writeHeader(file, header)
	if err != nil {
		return nil, err
	}
	return header, nil
}

// Check if the given file is corrupt, i.e does not correspond to the protocol contract
// If the file is valid, return pointers to both head element and tail element.
// Head and Tail pointers can be the same.
func checkCorrupt(file *os.File) (*header, error) {
	length, err := fileLength(file)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, err
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	header := &header{}
	currentOffset := int64(0)
	version, err := ReadInt(file, currentOffset)
	currentOffset += 4
	if err != nil {
		return nil, err
	}
	if version != MagicVersionNumber {
		return nil, CorruptVersionError
	}
	header.version = version
	// TODO(chermehdi): Use the flags
	flags, err := ReadInt(file, currentOffset)
	currentOffset += 4
	if err != nil {
		return nil, err
	}
	header.flags = flags

	elementCount, err := ReadLong(file, currentOffset)
	currentOffset += 8
	if err != nil {
		return nil, err
	}
	header.elementCount = elementCount

	headOffset, err := ReadLong(file, currentOffset)
	currentOffset += 8
	if err != nil {
		return nil, err
	}
	tailOffset, err := ReadLong(file, currentOffset)
	currentOffset += 8
	if err != nil {
		return nil, err
	}
	header.head = &elementPtr{
		offset: headOffset,
		length: 0,
	}
	header.tail = &elementPtr{
		offset: tailOffset,
		length: 0,
	}
	return header, nil
}

func fileLength(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return -1, err
	}
	return info.Size(), nil
}
