package utils

import "testing"

func TestRandomKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		key := GetTestKey(i)
		t.Log(string(key))
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		val := RandomValue(24)
		t.Log(string(val))
	}
}
