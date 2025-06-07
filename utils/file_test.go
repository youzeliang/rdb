package utils

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(dir)
	assert.Nil(t, err)
	assert.True(t, dirSize > 0)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	log.Println(size)
	assert.Nil(t, err)
	assert.True(t, size > 0)
}
