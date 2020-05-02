package eunomia

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadWriteInt(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	assert.NoError(t, writeInt(queueFile, 0, 12))
	assert.NoError(t, writeInt(queueFile, 4, 42))
	assert.NoError(t, writeInt(queueFile, 8, -12))
	assert.NoError(t, writeInt(queueFile, 12, int32(1)<<30))

	fileInfo, _ := queueFile.Stat()
	assert.Equal(t, int64(16), fileInfo.Size())

	val, err := readInt(queueFile, 0)
	assert.NoError(t, err)
	assert.Equal(t, int32(12), val)

	val, err = readInt(queueFile, 4)
	assert.NoError(t, err)
	assert.Equal(t, int32(42), val)

	val, err = readInt(queueFile, 8)
	assert.NoError(t, err)
	assert.Equal(t, int32(-12), val)

	val, err = readInt(queueFile, 12)
	assert.NoError(t, err)
	assert.Equal(t, int32(1)<<30, val)
}

func TestReadWriteLong(t *testing.T) {
	queueFile := createTestFile()
	defer deleteFile(queueFile)

	assert.NoError(t, writeLong(queueFile, 0, 12))
	assert.NoError(t, writeLong(queueFile, 8, 42))
	assert.NoError(t, writeLong(queueFile, 16, -12))
	assert.NoError(t, writeLong(queueFile, 24, int64(1)<<60))

	fileInfo, _ := queueFile.Stat()
	assert.Equal(t, int64(32), fileInfo.Size())

	val, err := readLong(queueFile, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), val)

	val, err = readLong(queueFile, 8)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), val)

	val, err = readLong(queueFile, 16)
	assert.NoError(t, err)
	assert.Equal(t, int64(-12), val)

	val, err = readLong(queueFile, 24)
	assert.NoError(t, err)
	assert.Equal(t, int64(1)<<60, val)
}
