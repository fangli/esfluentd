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
*
* You should have received a copy of the GNU General Public License along
* with this program; if not, see http://www.gnu.org/licenses/gpl-2.0.html
*************************************************************************/

package config

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	SYS_VER        string
	SYS_BUILD_VER  string
	SYS_BUILD_DATE string

	_datetimeLayout = []string{
		"{YYYY}", "2006",
		"{YY}", "06",
		"{MM}", "01",
		"{M}", "1",
		"{DD}", "02",
		"{D}", "2",
		"{hh}", "15",
		"{h}", "3",
		"{mm}", "04",
		"{m}", "4",
		"{ss}", "05",
		"{s}", "5",
	}

	_datetimeReplacer = strings.NewReplacer(_datetimeLayout...)
)

type Config struct {
	Listen        string
	Expires       time.Duration
	Nodes         []string
	Port          string
	IndicePattern string
	EsType        string
	TagField      string
	TimeField     string
	MaxDocs       int
	AutoDisc      bool
	SyncInterval  time.Duration
}

func (c *Config) Indice() string {
	return time.Now().UTC().Format(_datetimeReplacer.Replace(c.IndicePattern))
}

func Parse() *Config {
	tcplisten := flag.String("listen", "0.0.0.0:24224", "Which interface and port we'll listen to for client connnections")
	expires := flag.String("expires", "5m", "The timeout and expires of connections from clients, unit could be s, m, h or d")
	nodes := flag.String("hosts", "localhost", "Elasticsearch hosts, seperated by comma")
	port := flag.String("port", "9200", "Elasticsearch port")
	autodisc := flag.Bool("autodisc", false, "Auto refresh elasticsearch nodes and use cluster instead of the nodes you speicificed")
	indicepattern := flag.String("indice", "fluentd-{YYYY}.{MM}.{DD}", "Elasticsearch indice pattern, {YYYY}-{MM}-{DD} {hh}:{mm}:{ss}")
	estype := flag.String("type", "main", "The indice type of docs")
	tagfield := flag.String("tagfield", "", "The tag field name of the fluentd metrics")
	timefield := flag.String("timefield", "", "The timestamp field for the docs")
	maxdocs := flag.Int("max-docs", 1000, "Max number of Docs to hold in buffer before forcing flush")
	maxinterval := flag.String("max-interval", "1s", "Max delay before forcing a flush to Elasticearch")
	corenum := flag.Int("core", 0, "How many CPU cores to use. 0 for auto")

	version := flag.Bool("version", false, "Show version information")
	v := flag.Bool("v", false, "Show version information")

	flag.Parse()

	if *version || *v {
		fmt.Println("ESFluentd: A high performance fluentd forwarding server for elasticsearch")
		fmt.Println("Version", SYS_VER)
		fmt.Println("Build", SYS_BUILD_VER)
		fmt.Println("Compile at", SYS_BUILD_DATE)
		os.Exit(0)
	}

	syncinterval, err := time.ParseDuration(*maxinterval)
	if err != nil {
		log.Fatal("Unrecognized parameter for interval: ", *maxinterval, ". Use --help to get more information.")
	}

	connDuration, err := time.ParseDuration(*expires)
	if err != nil {
		log.Fatal("Unrecognized parameter for expires: ", *expires, ". Use --help to get more information.")
	}

	cfg := &Config{
		Listen:        *tcplisten,
		Nodes:         strings.Split(*nodes, ","),
		Port:          *port,
		IndicePattern: *indicepattern,
		EsType:        *estype,
		TagField:      *tagfield,
		TimeField:     *timefield,
		Expires:       connDuration,
		SyncInterval:  syncinterval,
		MaxDocs:       *maxdocs,
		AutoDisc:      *autodisc,
	}

	var coreNum int
	if *corenum == 0 {
		if runtime.NumCPU() > 1 {
			coreNum = runtime.NumCPU() - 1
		} else {
			coreNum = 1
		}
	} else {
		coreNum = *corenum
	}

	log.Printf("Configuration: %+v", *cfg)
	log.Println("CPU core number: " + strconv.Itoa(coreNum))
	runtime.GOMAXPROCS(coreNum)

	return cfg
}
