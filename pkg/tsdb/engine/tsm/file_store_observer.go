package tsm

type noFileStoreObserver struct{}

func (noFileStoreObserver) FileFinishing(path string) error { return nil }
func (noFileStoreObserver) FileUnlinking(path string) error { return nil }
