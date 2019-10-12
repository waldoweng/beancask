package errors

import "errors"

var (
	// ErrorSystemInternal system internal error, something should not happend happens
	ErrorSystemInternal = errors.New("system internal error")
	// ErrorDataNotFound data not found
	ErrorDataNotFound = errors.New("data not found")
	// ErrorReadFileData read wal file for record error
	ErrorReadFileData = errors.New("read file data error")
	// ErrorParseFileData parse wal file to record error
	ErrorParseFileData = errors.New("parse file data error")
	// ErrorIterateWalFile iterate throught wal file error
	ErrorIterateWalFile = errors.New("iterate over wal file error")
	// ErrorSystemShuttingDown system is shutting down, not write allow now
	ErrorSystemShuttingDown = errors.New("system is shutting down")
)
