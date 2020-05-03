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
	assert.Equal(t, queueWriter.head.offset, queueWriter.tail.offset)
	assert.Equal(t, queueWriter.head.length, queueWriter.tail.length)
}

func TestCheckCorrupt_WrongVersionNumber(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := queueFile.Write([]byte{1, 1, 1, 1})
	assert.NoError(t, err)

	_, _, err = checkCorrupt(queueFile)

	assert.Equal(t, CorruptVersion, err)
}

func TestCheckCorrupt_NoElementCount(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := queueFile.Write([]byte{0, 0, 0, 35})
	assert.NoError(t, err)

	_, _, err = checkCorrupt(queueFile)

	assert.Error(t, err)
}

func TestCheckCorrupt_ZeroElementCount(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := queueFile.Write([]byte{0, 0, 0, 35})
	_, err = queueFile.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	assert.NoError(t, err)

	tail, head, err := checkCorrupt(queueFile)

	assert.NoError(t, err)

	assert.NotNil(t, head)
	assert.NotNil(t, tail)
}

func TestCheckCorrupt_OneDataElementNotMatchingDataLength(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := queueFile.Write([]byte{0, 0, 0, 35})
	_, err = queueFile.Write([]byte{0, 0, 0, 0, 0, 0, 0, 1})
	mockData := &MockData{
		value: 14,
	}
	dataBuffer := mockData.Write()
	// The size of the data is not the same as the actual data, which implies that this file is corrupt.
	_, err = queueFile.Write(toBytes64(int64(len(dataBuffer)) + 1))
	_, err = queueFile.Write(dataBuffer)
	assert.NoError(t, err)

	_, _, err = checkCorrupt(queueFile)
	assert.Error(t, err)
}

func TestCheckCorrupt_OneDataElement(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := queueFile.Write([]byte{0, 0, 0, 35})
	_, err = queueFile.Write([]byte{0, 0, 0, 0, 0, 0, 0, 1})
	mockData := &MockData{
		value: 14,
	}
	dataBuffer := mockData.Write()
	_, err = queueFile.Write(toBytes64(int64(len(dataBuffer))))
	_, err = queueFile.Write(dataBuffer)
	assert.NoError(t, err)

	tail, head, err := checkCorrupt(queueFile)
	assert.NoError(t, err)
	assert.Equal(t, head, tail)
}

type MockData struct {
	value int32
}

func (m *MockData) Write() []byte {
	var buffer bytes.Buffer
	buffer.Write(toBytes(m.value))
	return buffer.Bytes()
}

func (m *MockData) Read(reader io.Reader) {
	result := make([]byte, 4)
	_, _ = reader.Read(result)
	m.value = (int32(result[0]&0xff) << 24) + (int32(result[1]&0xff) << 16) + (int32(result[2]&0xff) << 8) + (int32(result[0] & 0xff))
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
