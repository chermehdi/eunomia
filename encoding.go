package eunomia

import (
	"io"
)

// TODO(chermehdi): Fix the io.WriterAt and io.ReaderAt not many implementers.

// Write an string at the given offset.
// Writing a string is equivalent to writing 2 things:
//     1 - The length of the string			int64          (8 bytes)
//     2 - The actual bytes of the string   []byte		   (length bytes)
func WriteString(file io.WriterAt, offset int64, value string) (int64, error) {
	length := int64(len(value))
	if _, err := WriteLong(file, offset, length); err != nil {
		return -1, err
	}
	if _, err := WriteChunk(file, offset+8, []byte(value)); err != nil {
		return -1, err
	}
	return offset + length + 8, nil
}

// Read a string starting at the given offset.
// Reading a string is equivalent to reading 2 things:
//     1 - The length of the string			int64          (8 bytes)
//     2 - The actual bytes of the string   []byte		   (length bytes)
func ReadString(file io.ReaderAt, offset int64) (string, error) {
	length, err := ReadLong(file, offset)
	if err != nil {
		return "", err
	}
	data, err := ReadChunk(file, offset+8, length)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Write an int32 at the given offset.
func WriteInt(file io.WriterAt, offset int64, value int32) (int64, error) {
	buffer[0] = byte(value >> 24)
	buffer[1] = byte(value >> 16)
	buffer[2] = byte(value >> 8)
	buffer[3] = byte(value)
	written, err := file.WriteAt(buffer[0:4], offset)
	if err != nil {
		return -1, err
	}
	if written != 4 {
		return -1, UnexpectedNumberOfWrittenBytesError
	}
	return offset + 4, nil
}

// Write an int64 value in the given offset of the file.
// If the value cannot be written to the file an error is returned.
func WriteLong(file io.WriterAt, offset int64, value int64) (int64, error) {
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
		return -1, err
	}
	if written != 8 {
		return -1, UnexpectedNumberOfWrittenBytesError
	}
	return offset + 8, nil
}

// Read an int32 at the given offset
func ReadInt(file io.ReaderAt, offset int64) (int32, error) {
	buffer, err := ReadChunk(file, offset, 4)
	if err != nil {
		return -1, err
	}
	result := (int32(buffer[0]&0xff) << 24) + (int32(buffer[1]&0xff) << 16) + (int32(buffer[2]&0xff) << 8) + int32(buffer[3])
	return result, nil
}

// Read an int64 at the given offset
func ReadLong(file io.ReaderAt, offset int64) (int64, error) {
	buffer, err := ReadChunk(file, offset, 8)
	if err != nil {
		return -1, err
	}
	result := (int64(buffer[0]&0xff) << 56) + (int64(buffer[1]&0xff) << 48) + (int64(buffer[2]&0xff) << 40) + (int64(buffer[3]) << 32) + (int64(buffer[4]) << 24) + (int64(buffer[5]) << 16) + (int64(buffer[6]) << 8) + (int64(buffer[7]))
	return result, nil
}

// Read a chunk of data starting at the given offset and ending at offset + length - 1.
func ReadChunk(file io.ReaderAt, offset, length int64) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := file.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

// Write the given chunk o data at the given offset.
// If the data couldn't be written as a whole, an error is raised.
func WriteChunk(file io.WriterAt, offset int64, data []byte) (int, error) {
	written, err := file.WriteAt(data, offset)
	if err != nil {
		return -1, err
	}
	return written, nil
}

// Writes the passed header as specified by the protocol description to the
// queue file. If an error occurs during writing and error is returned.
func writeHeader(file io.WriterAt, header *header) error {
	currentOffset := int64(0)
	currentOffset, err := WriteInt(file, currentOffset, header.version)
	if err != nil {
		return err
	}
	currentOffset, err = WriteInt(file, currentOffset, header.flags)
	if err != nil {
		return err
	}
	currentOffset, err = WriteLong(file, currentOffset, header.elementCount)
	if err != nil {
		return err
	}
	currentOffset, err = WriteLong(file, currentOffset, header.head.offset)
	if err != nil {
		return err
	}
	currentOffset, err = WriteLong(file, currentOffset, header.tail.offset)
	if err != nil {
		return err
	}
	return nil
}
