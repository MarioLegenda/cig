package db

import (
	"fmt"
	"os"
	"sync"
)

var cm *concurrencyIndex

type index struct {
	index [10][2]int64
	file  *os.File
}

type concurrencyIndex struct {
	lock *sync.Mutex
	data map[string]*index
}

func (cm *concurrencyIndex) byName(name string) *index {
	lhd, ok := cm.data[name]
	if !ok {
		return nil
	}

	return lhd
}

func (cm *concurrencyIndex) close() error {
	for fileName, data := range cm.data {
		if err := data.file.Close(); err != nil {
			return fmt.Errorf("Cannot close file %s: %w", fileName, err)
		}
	}

	return nil
}

func addFileToConcurrentIndex(filePath string) {
	var mutex = &sync.Mutex{}

	mutex.Lock()
	if cm == nil {
		cm = &concurrencyIndex{data: make(map[string]*index), lock: mutex}
	}

	// handle indexing here
	mutex.Unlock()

}
