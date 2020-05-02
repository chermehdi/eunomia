package eunomia

import (
	"github.com/stretchr/testify/assert"
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
