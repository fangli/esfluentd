/*************************************************************************
* This file is a part of esfluentd, A high performance fluentd forwarding
* server for elasticsearch, supporting cluster and HA of elasticsearch.

* Copyright (C) 2014  Fang Li <surivlee@gmail.com> and Funplus, Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*`
* You should have received a copy of the GNU General Public License along
* with this program; if not, see http://www.gnu.org/licenses/gpl-2.0.html
*************************************************************************/

package logging

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	STDOUT       = 0
	FILE         = 1
	STD_AND_FILE = 2
)

const (
	DEBUG int = iota
	INFO
	WARNING
	ERROR
	FATAL
)

var LevelStr = []string{"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"}

var LevelInt = map[string]int{
	"DEBUG":   0,
	"INFO":    1,
	"WARNING": 2,
	"ERROR":   3,
	"FATAL":   4,
}

type Log struct {
	Dest     int
	Level    int
	FileName string
	f        *os.File
	lock     sync.Mutex
}

func (l *Log) write(level int, msg string) {
	var err error
	if l.Level > level {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	output := fmt.Sprintf(
		"%s [%s] %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		LevelStr[level],
		msg,
	)
	if (l.Dest == 0) || (l.Dest == 2) {
		fmt.Print(output)
	}
	if (l.Dest == 1) || (l.Dest == 2) {
		l.f, err = os.OpenFile(l.FileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return
		}
		defer l.f.Close()
		l.f.WriteString(output)
	}
}

func (l *Log) Debug(msg string) {
	l.write(DEBUG, msg)
}

func (l *Log) Info(msg string) {
	l.write(INFO, msg)
}

func (l *Log) Warning(msg string) {
	l.write(WARNING, msg)
}

func (l *Log) Error(msg string) {
	l.write(ERROR, msg)
}

func (l *Log) Fatal(msg string) {
	l.write(FATAL, msg)
	l.write(FATAL, "Exited with return code 1.")
	os.Exit(1)
}
