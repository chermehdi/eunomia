package eunomia

import "os"

// Returns true if the file does not exist, or empty
func fileExist(file *os.File) bool {
	info, err := os.Stat(file.Name())
	if err != nil {
		return os.IsExist(err)
	}
	return info.Size() > 0
}
