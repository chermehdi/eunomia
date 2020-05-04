package eunomia

import "os"

func WriteInt(file *os.File, offset int64, value int32) error {
	buffer[0] = byte(value >> 24)
	buffer[1] = byte(value >> 16)
	buffer[2] = byte(value >> 8)
	buffer[3] = byte(value)
	written, err := file.WriteAt(buffer[0:4], offset)
	if err != nil {
		return err
	}
	if written != 4 {
		return UnexpectedNumberOfWrittenBytesError
	}
	return nil
}

// Write an int64 value in the given offset of the file.
// If the value cannot be written to the file an error is returned.
func WriteLong(file *os.File, offset int64, value int64) error {
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
		return UnexpectedNumberOfWrittenBytesError
	}
	return nil
}

func ReadInt(file *os.File, offset int64) (int32, error) {
	buffer, err := ReadChunk(file, offset, 4)
	if err != nil {
		return -1, err
	}
	result := (int32(buffer[0]&0xff) << 24) + (int32(buffer[1]&0xff) << 16) + (int32(buffer[2]&0xff) << 8) + int32(buffer[3])
	return result, nil
}

func ReadLong(file *os.File, offset int64) (int64, error) {
	buffer, err := ReadChunk(file, offset, 8)
	if err != nil {
		return -1, err
	}
	result := (int64(buffer[0]&0xff) << 56) + (int64(buffer[1]&0xff) << 48) + (int64(buffer[2]&0xff) << 40) + (int64(buffer[3]) << 32) + (int64(buffer[4]) << 24) + (int64(buffer[5]) << 16) + (int64(buffer[6]) << 8) + (int64(buffer[7]))
	return result, nil
}

func ReadChunk(file *os.File, offset, length int64) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := file.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func WriteChunk(file *os.File, offset int64, data []byte) (int, error) {
	written, err := file.WriteAt(data, offset)
	if err != nil {
		return -1, err
	}
	return written, nil
}

// Writes the passed header as specified by the protocol description to the
// queue file. If an error occurs during writing and error is returned.
func writeHeader(file *os.File, header *header) error {
	currentOffset := int64(0)
	err := WriteInt(file, currentOffset, header.version)
	currentOffset += 4
	if err != nil {
		return err
	}
	err = WriteInt(file, currentOffset, header.flags)
	currentOffset += 4
	if err != nil {
		return err
	}
	err = WriteLong(file, currentOffset, header.elementCount)
	currentOffset += 8
	if err != nil {
		return err
	}
	err = WriteLong(file, currentOffset, header.head.offset)
	currentOffset += 8
	if err != nil {
		return err
	}
	err = WriteLong(file, currentOffset, header.tail.offset)
	currentOffset += 8
	if err != nil {
		return err
	}
	return nil
}
