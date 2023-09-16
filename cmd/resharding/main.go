/*
 * Copyright [2023] [studyzy(studyzy@gmail.com)]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/studyzy/shardingdb"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	var input string
	var output string
	var logType int
	var replication int

	flag.IntVar(&logType, "l", 0, "log type: 0(default) - no log, 1 - summary log, 2 - detail log")
	flag.IntVar(&replication, "r", 1, "replication factor, default 1")
	flag.StringVar(&input, "i", "", "input: leveldb path list, separated by comma")
	flag.StringVar(&output, "o", "", "output: new leveldb path list, separated by comma")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -i [old leveldb path] -o [new leveldb path]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Keep old leveldb path resharding example: %s -i /data1,/newfolder1\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Resharding old leveldb to new path example, and print summary log: %s -i /data1 -o /newfolder1,/newfolder2 -l 1\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if input == "" {
		fmt.Println("-i parameters are required.")
		return
	}
	inputPathList := strings.Split(input, ",")

	outputPath := strings.Split(output, ",")
	fmt.Printf("Resharding start, input: %v, output: %v\n", inputPathList, outputPath)
	startTime := time.Now()
	if len(outputPath) == 0 || output == "" {
		sdb, err := shardingdb.NewShardingDb(
			shardingdb.WithDbPaths(inputPathList...),
			shardingdb.WithShardingFunc(shardingdb.MurmurSharding),
			shardingdb.WithLogger(getLogger(logType)),
			shardingdb.WithReplication(uint16(replication)),
		)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer sdb.Close()
		err = sdb.Resharding()
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		inputs := make([]shardingdb.LevelDbHandle, len(inputPathList))
		for i := 0; i < len(inputPathList); i++ {
			db, err := leveldb.OpenFile(inputPathList[i], nil)
			if err != nil {
				fmt.Println(err)
				return
			}
			defer db.Close()
			inputs[i] = db
		}
		sdb, err := shardingdb.NewShardingDb(
			shardingdb.WithDbPaths(outputPath...),
			shardingdb.WithShardingFunc(shardingdb.MurmurSharding),
			shardingdb.WithLogger(getLogger(logType)),
			shardingdb.WithReplication(uint16(replication)),
		)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer sdb.Close()
		err = shardingdb.Migration(inputs, sdb)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	fmt.Printf("Resharding finished, cost %v\n", time.Now().Sub(startTime))
}

func getLogger(logType int) shardingdb.Logger {
	switch logType {
	case 0:
		return nil
	case 1:
		return &summaryLogger{period: 10000}
	case 2:
		return &consoleLogger{}
	default:
		return nil
	}
}

type consoleLogger struct {
}

func (l *consoleLogger) Debug(msg string) {
	fmt.Println("[DEBUG] ", msg)
}
func (l *consoleLogger) Info(msg string) {
	fmt.Println("[INFO] ", msg)
}

type summaryLogger struct {
	count  uint64
	period uint64
}

func (l *summaryLogger) Debug(msg string) {
	l.count++
	if l.count%l.period == 0 {
		fmt.Println("[INFO] processed ", l.count, " k/vs")
	}
}
func (l *summaryLogger) Info(msg string) {
	fmt.Println("[INFO] ", msg)
	fmt.Println("[INFO] processed ", l.count, " k/vs")
}
