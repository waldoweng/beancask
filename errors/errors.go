package errors

import "errors"

var (
	// ErrorSystemInternal for
	ErrorSystemInternal = errors.New("system internal error")
	// ErrorDataNotFound for
	ErrorDataNotFound = errors.New("data not found")
	// ErrorReadFileData for
	ErrorReadFileData = errors.New("read file data error")
	// ErrorParseFileData for
	ErrorParseFileData = errors.New("parse file data error")
	// ErrorIterateWalFile for
	ErrorIterateWalFile = errors.New("iterate over wal file error")
)
