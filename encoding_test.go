package eunomia

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadWriteInt(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)
	currentOffset, err := WriteInt(queueFile, 0, 12)
	assert.NoError(t, err)
	currentOffset, err = WriteInt(queueFile, currentOffset, 42)
	assert.NoError(t, err)
	currentOffset, err = WriteInt(queueFile, currentOffset, -12)
	assert.NoError(t, err)
	currentOffset, err = WriteInt(queueFile, currentOffset, int32(1)<<30)
	assert.NoError(t, err)

	fileInfo, _ := queueFile.Stat()
	assert.Equal(t, int64(16), fileInfo.Size())

	val, err := ReadInt(queueFile, 0)
	assert.NoError(t, err)
	assert.Equal(t, int32(12), val)

	val, err = ReadInt(queueFile, 4)
	assert.NoError(t, err)
	assert.Equal(t, int32(42), val)

	val, err = ReadInt(queueFile, 8)
	assert.NoError(t, err)
	assert.Equal(t, int32(-12), val)

	val, err = ReadInt(queueFile, 12)
	assert.NoError(t, err)
	assert.Equal(t, int32(1)<<30, val)
}

func TestReadWriteLong(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	currentOffset, err := WriteLong(queueFile, 0, 12)
	assert.NoError(t, err)
	currentOffset, err = WriteLong(queueFile, currentOffset, 42)
	assert.NoError(t, err)
	currentOffset, err = WriteLong(queueFile, currentOffset, -12)
	assert.NoError(t, err)
	currentOffset, err = WriteLong(queueFile, currentOffset, int64(1)<<60)
	assert.NoError(t, err)

	fileInfo, _ := queueFile.Stat()
	assert.Equal(t, int64(32), fileInfo.Size())

	val, err := ReadLong(queueFile, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), val)

	val, err = ReadLong(queueFile, 8)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val)

	val, err = ReadLong(queueFile, 16)
	assert.NoError(t, err)
	assert.Equal(t, int64(-12), val)

	val, err = ReadLong(queueFile, 24)
	assert.NoError(t, err)
	assert.Equal(t, int64(1)<<60, val)
}

func TestReadWriteString_EmptyString(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	_, err := WriteString(queueFile, 0, "")
	assert.NoError(t, err)
	str, err := ReadString(queueFile, 0)
	assert.NoError(t, err)
	assert.Equal(t, "", str)
}

func TestReadWriteString_SimpleString(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	targetString := "Hello world!"
	_, err := WriteString(queueFile, 0, targetString)
	assert.NoError(t, err)
	str, err := ReadString(queueFile, 0)
	assert.NoError(t, err)
	assert.Equal(t, targetString, str)
}

func TestReadWriteString_UnicodeString(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	targetString := "😇 Hello unicode characters"
	_, err := WriteString(queueFile, 0, targetString)
	assert.NoError(t, err)
	str, err := ReadString(queueFile, 0)
	assert.NoError(t, err)
	assert.Equal(t, targetString, str)
}
