package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().UnixNano()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey 生成测试使用的 key
func GetTestKey(n int) []byte {
	return []byte(fmt.Sprintf("go-key-%09d", n))
}

// RandomValue 这里生成随机 value，用于测试
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("go-value-" + string(b))
}
