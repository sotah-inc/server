package database

import (
	"fmt"
)

// keying
func metaKeyName(name string) []byte {
	return []byte(name)
}

// bucketing
func metaBucketName(name string) []byte {
	return []byte(name)
}

// db
func metaDatabaseFilePath(dirPath string) string {
	return fmt.Sprintf("%s/meta.db", dirPath)
}
