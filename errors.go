package rdb

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch num")
	ErrMergeInProgress        = errors.New("merge is in progress, try again later")
	ErrMergeRatioUnreached    = errors.New("the merge ratio do not reach the option")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough disk space for merge")
	ErrDatabaseIsUsing        = errors.New("database directory is using by another process")
)
