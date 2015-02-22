package Cache

import (
	"fmt"
	"strconv"
)

const ITEMS_PER_BUCKET = 1024

func GetBucketKey(id int64, namespace string) (string, string) {
	remainder := id % ITEMS_PER_BUCKET
	bucketId := (id - remainder) / ITEMS_PER_BUCKET

	bucketKey := fmt.Sprintf("%s_bucket:%d", namespace, bucketId)
	subKey := strconv.FormatInt(remainder, 10)
	return bucketKey, subKey
}
