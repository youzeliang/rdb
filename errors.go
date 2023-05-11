package rdb

import "errors"

var ErrKeyIsEmpty = errors.New("key is empty")
var ErrIndexUpdate = errors.New("index update error")
var ErrKeyNotFound = errors.New("key not found")
var ErrDataFileNotFound = errors.New("data file not found")
