package data

type LogRecordPos struct {
	Fid    uint32 // file id
	Offset int64  // Offset refers to the position where data is stored in a data file.
}
