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

package server

import (
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/fangli/esfluentd/config"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/cluster"
	"github.com/mattbaird/elastigo/core"
	"github.com/vmihailenco/msgpack"
)

type Server struct {
	Cfg       *config.Config
	metricChn chan interface{}
}

func (s *Server) EsInsert() {
	indexer := core.NewBulkIndexerErrors(10, 2)
	indexer.BulkMaxBuffer = 10485760
	indexer.BulkMaxDocs = s.Cfg.MaxDocs
	indexer.BufferDelayMax = s.Cfg.SyncInterval
	done := make(chan bool)
	indexer.Run(done)

	go func() {
		for _ = range indexer.ErrorChannel {
		}
	}()

	for metric := range s.metricChn {
		indexer.Index(s.Cfg.Indice(), s.Cfg.EsType, "", "", nil, &metric, false)
	}
}

func (s *Server) clientHandler(conn net.Conn) {
	var err error
	defer conn.Close()
	decoder := msgpack.NewDecoder(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(s.Cfg.Expires))
		var data [3]interface{}
		err = decoder.Decode(&data)
		if err != nil {
			if err != io.EOF {
				log.Println(err.Error())
			}
			return
		}

		body := make(map[string]interface{})
		for k, v := range data[2].(map[interface{}]interface{}) {
			body[k.(string)] = v
		}
		if s.Cfg.TagField != "" {
			body[s.Cfg.TagField] = data[0].(string)
		}
		if s.Cfg.TimeField != "" {
			body[s.Cfg.TimeField] = data[1].(int64) * 1000
		}

		s.metricChn <- body

	}
}

func (s *Server) serveTcp() {
	var err error
	ln, err := net.Listen("tcp4", s.Cfg.Listen)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting connections: " + err.Error())
			continue
		}
		go s.clientHandler(conn)
	}
}

func (s *Server) InitialNodes() {
	api.SetHosts(s.Cfg.Nodes)
	if s.Cfg.AutoDisc {
		s.RefreshNodes()
		go func() {
			for {
				time.Sleep(time.Hour)
				s.RefreshNodes()
			}
		}()
	}
}

func (s *Server) RefreshNodes() {
	cs, err := cluster.ClusterState(cluster.ClusterStateFilter{FilterNodes: true})
	if err != nil {
		return
	}
	nodes := []string{}
	reg := regexp.MustCompile(`(\d{1,3}\.){3}\d{1,3}`)
	for _, data := range cs.Nodes {
		nodes = append(nodes, reg.FindAllString(data.TransportAddress, 1)[0]+":"+s.Cfg.Port)
	}
	api.SetHosts(nodes)
	log.Println("Elasticsearch cluster nodes: " + strings.Join(nodes, ", "))
}

func (s *Server) Forever() {
	s.metricChn = make(chan interface{}, 500000)
	s.InitialNodes()
	go s.EsInsert()
	s.serveTcp()
}
