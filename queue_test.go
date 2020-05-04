package eunomia

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"testing"
)

func TestNewQueueWriter(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	queueWriter, err := NewQueueWriter(queueFile)
	assert.NoError(t, err)
	assert.Equal(t, queueWriter.header.head.offset, queueWriter.header.tail.offset)
	assert.Equal(t, queueWriter.header.head.length, queueWriter.header.tail.length)
}

func TestCheckCorrupt_WrongVersionNumber(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := queueFile.Write([]byte{1, 1, 1, 1})
	assert.NoError(t, err)

	_, err = checkCorrupt(queueFile)

	assert.Equal(t, CorruptVersionError, err)
}

func TestCheckCorrupt_NoElementCount(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	//version
	_, err := queueFile.Write([]byte{0, 0, 0, 35})
	// flags
	_, err = queueFile.Write([]byte{0, 0, 0, 0})
	assert.NoError(t, err)

	_, err = checkCorrupt(queueFile)

	assert.Error(t, err)
}

func TestCheckCorrupt_ZeroElementCount(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	header := &header{
		elementCount: 0,
		version:      MagicVersionNumber,
		flags:        0,
		head: &elementPtr{
			offset: 64,
			length: 0,
		},
		tail: &elementPtr{
			offset: 64,
			length: 0,
		},
	}
	err := writeHeader(queueFile, header)
	assert.NoError(t, err)

	header2, err := checkCorrupt(queueFile)

	assert.NoError(t, err)
	assert.Equal(t, header.elementCount, header2.elementCount)
	assert.NotNil(t, header.head)
	assert.NotNil(t, header.tail)
}

func TestCheckCorrupt_OneDataElementNotMatchingDataLength(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := queueFile.Write([]byte{0, 0, 0, 35})
	_, err = queueFile.Write([]byte{0, 0, 0, 0, 0, 0, 0, 1})
	mockData := MockData{
		value: 14,
	}
	dataBuffer := (&MockDataSerializer{}).Write(mockData)
	// The size of the data is not the same as the actual data, which implies that this file is corrupt.
	_, err = queueFile.Write(toBytes64(int64(len(dataBuffer)) + 1))
	_, err = queueFile.Write(dataBuffer)
	assert.NoError(t, err)

	_, err = checkCorrupt(queueFile)
	assert.Error(t, err)
}

func TestCheckCorrupt_OneDataElement(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)
	mockData := MockData{
		value: 14,
	}
	dataBuffer := (&MockDataSerializer{}).Write(mockData)
	header := &header{
		elementCount: 1,
		version:      MagicVersionNumber,
		flags:        0,
		head: &elementPtr{
			offset: 32,
			length: -1,
		},
		tail: &elementPtr{
			offset: 32,
			length: -1,
		},
	}
	err := writeHeader(queueFile, header)
	WriteLong(queueFile, header.tail.offset, int64(len(dataBuffer)))
	WriteChunk(queueFile, header.tail.offset+8, dataBuffer)
	header.head.length = int64(len(dataBuffer))
	header.tail.length = int64(len(dataBuffer))
	header2, err := checkCorrupt(queueFile)
	assert.NoError(t, err)
	assert.Equal(t, header2.head.offset, header.tail.offset)
}

func TestFileQueue_Push2elements(t *testing.T) {
	queue, err := NewFileQueue("some-queue", &MockDataSerializer{})
	defer queue.Delete()

	assert.NoError(t, err)
	assert.Equal(t, int64(0), queue.Size())

	queue.Push(&MockData{value: 12})
	queue.Push(&MockData{value: 13})

	assert.Equal(t, int64(2), queue.Size())

	fq := queue.(*FileQueue)
	assert.Equal(t, int64(32), fq.writer.header.head.offset)
	assert.Equal(t, int64(44), fq.writer.header.tail.offset)
}

func TestFileQueue_Push(t *testing.T) {
	queue, err := NewFileQueue("some-queue", &MockDataSerializer{})
	defer queue.Delete()

	assert.NoError(t, err)
	assert.Equal(t, int64(0), queue.Size())

	queue.Push(&MockData{value: 12})

	assert.Equal(t, int64(1), queue.Size())
}

func TestFileQueue_PeekEmptyQueue(t *testing.T) {
	queue, err := NewFileQueue("some-queue", &MockDataSerializer{})
	defer queue.Delete()

	assert.NoError(t, err)

	_, err = queue.Peek()
	assert.Same(t, EmptyQueueError, err)
}

func TestFileQueue_PeekQueue(t *testing.T) {
	queue, err := NewFileQueue("some-queue", &MockDataSerializer{})
	defer queue.Delete()

	assert.NoError(t, err)
	mockData := MockData{123}

	queue.Push(&mockData)

	el, err := queue.Peek()

	assert.NoError(t, err)
	mockDataInstance := (el).(MockData)
	assert.Equal(t, mockData.value, mockDataInstance.value)
}

// tests Utilities
type MockData struct {
	value int32
}

type MockDataSerializer struct {
}

func (m *MockDataSerializer) Write(i interface{}) []byte {
	var buffer bytes.Buffer
	data, ok := i.(MockData)
	if !ok {
		dataPtr, ok := i.(*MockData)
		if !ok {
			panic("Unexpected type")
		}
		buffer.Write(toBytes(dataPtr.value))
		return buffer.Bytes()
	}
	buffer.Write(toBytes(data.value))
	return buffer.Bytes()
}

func (m *MockDataSerializer) Read(reader io.Reader) interface{} {
	result := make([]byte, 4)
	_, _ = reader.Read(result)
	mockData := MockData{
		value: (int32(result[0]&0xff) << 24) + (int32(result[1]&0xff) << 16) + (int32(result[2]&0xff) << 8) + (int32(result[3] & 0xff)),
	}
	return mockData
}

func toBytes64(value int64) []byte {
	buffer := make([]byte, 8)
	buffer[0] = byte(value >> 56)
	buffer[1] = byte(value >> 48)
	buffer[2] = byte(value >> 40)
	buffer[3] = byte(value >> 32)
	buffer[4] = byte(value >> 24)
	buffer[5] = byte(value >> 16)
	buffer[6] = byte(value >> 8)
	buffer[7] = byte(value)
	return buffer
}
func toBytes(value int32) []byte {
	result := make([]byte, 4)
	result[0] = byte(value >> 24)
	result[1] = byte(value >> 16)
	result[2] = byte(value >> 8)
	result[3] = byte(value)
	return result
}

// removes the queue test file
func deleteFile(file *os.File) {
	os.Remove(file.Name())
}

func createTestFile() *os.File {
	file, err := os.OpenFile("queue", os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	return file
}
