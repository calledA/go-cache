package database

import "strings"

/**
 * @Author: wanglei
 * @File: router
 * @Version: 1.0.0
 * @Description:
 * @Date: 2023/07/20 16:30
 */

var cmdTable = make(map[string]*command)

const (
	flagWrite    = 0
	flagReadOnly = 1
)

type command struct {
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
	arity    int
	flags    int
}

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, undo UndoFunc, arity int, flag int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     undo,
		arity:    arity,
		flags:    flag,
	}
}

func isReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd := cmdTable[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&flagWriteOnly > 0
}
