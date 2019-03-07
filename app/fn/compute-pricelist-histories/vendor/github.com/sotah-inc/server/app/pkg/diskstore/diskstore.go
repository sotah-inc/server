package diskstore

func NewDiskStore(cacheDir string) DiskStore { return DiskStore{CacheDir: cacheDir} }

type DiskStore struct {
	CacheDir string
}
