package wal

type WAL struct{

}

func OpenWAL(path string) *WAL{
	return &WAL{}
}

func (w *WAL) Write(key string, value []byte) error{
	return nil
}