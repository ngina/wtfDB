package memory

type EvictionPolicy interface {
	Evict() (Frame, error)
}