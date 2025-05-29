package utils

import (
	"strconv"
)

func FloatFromBytes(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}

func Float64ToBytes(float float64) []byte {
	return []byte(strconv.FormatFloat(float, 'f', -1, 64))
}
