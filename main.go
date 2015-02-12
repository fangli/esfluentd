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

package main

import (
	"./config"
	"./server"
)

func main() {
	cfg := config.Parse()
	s := server.Server{
		Cfg: cfg,
	}
	s.Forever()
}
