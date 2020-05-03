package eunomia

import (
	"errors"
	"io"
	"os"
)

var buffer = make([]byte, 64)

var NewFileError = errors.New("file has never been initialized")
var CorruptVersion = errors.New("invalid version in the file header")
var UnexpectedNumberOfWrittenBytes = errors.New("the number of written bytes and the number of expected bytes to be written is different")

// Magic number to act as the version
const MagicVersionNumber int32 = 0x23

type Queue interface {
	Push(element QueueElement)

	Poll() (QueueElement, error)

	Size() int64
}

// Types that we can push onto the queue should adhere to the following contract:
//   Write: An element should be responsible of writing itself as a sequence of bytes.
//    (Note: This implies a temporary buffer is allocated this should be taken care of in the next iteration)
//
//   Read: An element should be able to restore it's state from a given io.Reader
type Serializable interface {
	Write() []byte

	Read(reader io.Reader)
}

type QueueElement interface {
	Serializable
}

type FileQueue struct {
	filePath string
	writer   *QueueProtocolWriter
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
	head        *elementPtr
	tail        *elementPtr
}

// Pointer to some data element in the file
// Each element is identified by it's start position and it's length (in bytes).
// The elements are written [elementLength,elementData] and the offset points to the first
// byte of the elementLength, i.e when reading any element, the data starts at offset + 8 and not at offset
type elementPtr struct {
	offset int64
	length int64
}

func NewQueueWriter(backingFile *os.File) (*QueueProtocolWriter, error) {
	writer := &QueueProtocolWriter{
		backingFile: backingFile,
	}
	head, tail, err := checkCorrupt(backingFile)

	if err == NewFileError {
		err = writeInt(backingFile, 0, MagicVersionNumber)
		if err != nil {
			return nil, err
		}
		err = writeLong(backingFile, int64(4), int64(0))
		if err != nil {
			return nil, err
		}
		head = &elementPtr{
			offset: 12,
			length: 0,
		}
		tail = &elementPtr{
			offset: 12,
			length: 0,
		}
	}
	if err != nil {
		return nil, err
	}
	writer.head = head
	writer.tail = tail
	return writer, nil
}

func writeInt(file *os.File, offset int64, value int32) error {
	buffer[0] = byte(value >> 24)
	buffer[1] = byte(value >> 16)
	buffer[2] = byte(value >> 8)
	buffer[3] = byte(value)
	written, err := file.WriteAt(buffer[0:4], offset)
	if err != nil {
		return err
	}
	if written != 4 {
		return UnexpectedNumberOfWrittenBytes
	}
	return nil
}

// Write an int64 value in the given offset of the file.
// If the value cannot be written to the file an error is returned.
func writeLong(file *os.File, offset int64, value int64) error {
	buffer[0] = byte(value >> 56)
	buffer[1] = byte(value >> 48)
	buffer[2] = byte(value >> 40)
	buffer[3] = byte(value >> 32)
	buffer[4] = byte(value >> 24)
	buffer[5] = byte(value >> 16)
	buffer[6] = byte(value >> 8)
	buffer[7] = byte(value)
	written, err := file.WriteAt(buffer[0:8], offset)
	if err != nil {
		return err
	}
	if written != 8 {
		return UnexpectedNumberOfWrittenBytes
	}
	return nil
}

func readInt(file *os.File, offset int64) (int32, error) {
	buffer, err := readChunk(file, offset, 4)
	if err != nil {
		return -1, err
	}
	result := (int32(buffer[0]&0xff) << 24) + (int32(buffer[1]&0xff) << 16) + (int32(buffer[2]&0xff) << 8) + int32(buffer[3])
	return result, nil
}

func readLong(file *os.File, offset int64) (int64, error) {
	buffer, err := readChunk(file, offset, 8)
	if err != nil {
		return -1, err
	}
	result := (int64(buffer[0]&0xff) << 56) + (int64(buffer[1]&0xff) << 48) + (int64(buffer[2]&0xff) << 40) + (int64(buffer[3]) << 32) + (int64(buffer[4]) << 24) + (int64(buffer[5]) << 16) + (int64(buffer[6]) << 8) + (int64(buffer[7]))
	return result, nil
}

func readChunk(file *os.File, offset, length int64) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := file.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

// Check if the given file is corrupt, i.e does not correspond to the protocol contract
// If the file is valid, return pointers to both head element and tail element.
// Head and Tail pointers can be the same.
func checkCorrupt(file *os.File) (head, tail *elementPtr, corruptErr error) {
	length, err := fileLength(file)
	if err != nil {
		corruptErr = err
		return
	}
	if length == 0 {
		corruptErr = NewFileError
		return
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		corruptErr = err
		return
	}
	version, err := readInt(file, 0)
	if err != nil {
		corruptErr = err
		return
	}
	if version != MagicVersionNumber {
		corruptErr = CorruptVersion
		return
	}
	elementCount, err := readLong(file, 4)
	if err != nil {
		corruptErr = err
		return
	}
	if elementCount == 0 {
		// The queue is empty
		head = &elementPtr{
			offset: 12,
			length: 0,
		}
		tail = &elementPtr{
			offset: 12,
			length: 0,
		}
		return
	}
	currOffset := int64(12)
	for i := int64(0); i < elementCount; i++ {
		dataLength, err := readLong(file, currOffset)
		if err != nil {
			corruptErr = err
			return
		}
		if i == 0 {
			head = &elementPtr{
				offset: currOffset,
				length: dataLength,
			}
		}
		tail = &elementPtr{
			offset: currOffset,
			length: dataLength,
		}
		currOffset += 8
		_, err = readChunk(file, currOffset, dataLength)
		if err != nil {
			corruptErr = err
			return
		}
		currOffset += dataLength
	}
	return
}

func fileLength(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return -1, err
	}
	return info.Size(), nil
}
