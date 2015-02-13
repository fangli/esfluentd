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
	"encoding/json"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"../config"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/cluster"
	"github.com/mattbaird/elastigo/core"
	"github.com/sendgridlabs/go-kinesis"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Server struct {
	Cfg              *config.Config
	metricChn        chan interface{}
	metricKinesisChn chan interface{}
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

func (s *Server) doKinesisInsert(ksis *kinesis.Kinesis, cache map[int]interface{}) {
	var args = kinesis.NewArgs()
	args.Add("StreamName", s.Cfg.AWSKinesisStreamName)

	var vTimestamp string
	var vReceiveTime string
	var vPartitionKey string
	var size = 0
	for _, metric := range cache {
		data, err := json.Marshal(metric)
		if err != nil {
			log.Println("Error Marshal: " + err.Error())
		}
		size += len(data)

		item, _ := metric.(map[string]interface{})
		if val, ok := item["namespace"]; ok {
			if val != nil {
				vPartitionKey = val.(string)
			} else {
				return
			}
		} else {
			return
		}
		if val, ok := item["timestamp"]; ok {
			if val != nil {
				vTimestamp = time.Unix(val.(int64), 0).String()
			} else {
				return
			}
		} else {
			return
		}
		if val, ok := item["receivetime"]; ok {
			if val != nil {
				vReceiveTime = time.Unix(val.(int64), 0).String()
			} else {
				return
			}
		} else {
			return
		}

		args.AddRecord(data, vPartitionKey)
	}
	now := time.Now().Unix()
	vPutRecordTime := time.Unix(now, 0).String()

	var err error
	_, err = ksis.PutRecords(args)
	if err != nil {
		if err != io.EOF {
			log.Println("PutRecord Error: " + err.Error() + "PutRecordSize: " + strconv.Itoa(size))
		}
		return
	} else {
		log.Println("PutRecordSize: " + strconv.Itoa(size) + ", ReceiveTime: " + vReceiveTime + ", PutRecordTime: " + vPutRecordTime + ", Timestamp: " + vTimestamp)
	}
}

func (s *Server) KinesisInsert() {
	auth := kinesis.Auth{AccessKey: s.Cfg.AWSKinesisAccessKey, SecretKey: s.Cfg.AWSKinesisSecretKey}
	region := kinesis.Region{Name: s.Cfg.AWSKinesisRegion}
	var ksis = kinesis.New(&auth, region)

	var size = 0
	var count = 0
	var cache = make(map[int]interface{})
	for {
		select {
		case metric, ok := <-s.metricKinesisChn:
			if ok {
				data, err := json.Marshal(metric)
				if err != nil {
					log.Println("Error Marshal: " + err.Error())
				}

				n := len(data)
				size += n

				if size < 1024*50 {
					if metric != nil {
						cache[count] = metric
						count += 1
					} else {
						continue
					}
				} else {
					s.metricKinesisChn <- metric

					go s.doKinesisInsert(ksis, cache)
					size = 0
					count = 0
					cache = make(map[int]interface{})
				}
			} else {
				log.Println("Error kinesis channel closed!!!")
			}
		default:
			if len(cache) != 0 {
				go s.doKinesisInsert(ksis, cache)
				size = 0
				count = 0
				cache = make(map[int]interface{})
				time.Sleep(100 * time.Millisecond)
			}
		}
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

		name := data[0].(string)
		content := make(map[string]interface{})
		for k, v := range data[2].(map[interface{}]interface{}) {
			content[k.(string)] = v
		}
		content["namespace"] = content["instanceid"]
		delete(content, "instanceid")
		content["timestamp"] = data[1].(int64)
		value := content["_value"]
		delete(content, "_value")
		content["receivetime"] = time.Now().Unix()

		metricKinesisJson := map[string]interface{}{"name": name, "value": value, "type": "number", "cycle": 30}
		var rbody []map[string]interface{}
		rbody = append(rbody, metricKinesisJson)
		content["metrics"] = rbody

		s.metricChn <- body
		s.metricKinesisChn <- content

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
	s.metricKinesisChn = make(chan interface{}, 500000)
	s.InitialNodes()
	go s.EsInsert()
	go s.KinesisInsert()
	s.serveTcp()
}
