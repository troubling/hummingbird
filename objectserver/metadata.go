package objectserver

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// MetadataHash returns a hash of the contents of the metadata.
func MetadataHash(metadata map[string]string) string {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hasher := fnv.New64a()
	for _, key := range keys {
		hasher.Write([]byte(key))
		hasher.Write([]byte{0})
		hasher.Write([]byte(metadata[key]))
		hasher.Write([]byte{0})
	}
	return fmt.Sprintf("%016x", hasher.Sum64())
}

// MetadataMerge will return the result of merging the a and b metadata sets;
// neither a nor b should be used after calling this method.
func MetadataMerge(a map[string]string, b map[string]string) map[string]string {
	if a["X-Timestamp"] < b["X-Timestamp"] {
		a, b = b, a
	}
	for _, key := range []string{"Content-Length", "Content-Type", "deleted", "ETag"} {
		if _, ok := a[key]; !ok {
			if value, ok := b[key]; ok {
				a[key] = value
			}
		}
	}
	for key, value := range b {
		if strings.HasPrefix(key, "X-Object-Sysmeta-") {
			if _, ok := a[key]; !ok {
				a[key] = value
			}
		}
	}
	return a
}
