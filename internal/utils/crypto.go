package utils

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
)

func HashFileMd5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := md5.New()

	// Stream the file into the hasher in small chunks automatically
	// io.Copy uses a small internal buffer (typically 32KB) to read and write sequentially
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	hashBytes := hasher.Sum(nil)
	return hex.EncodeToString(hashBytes), nil
}
