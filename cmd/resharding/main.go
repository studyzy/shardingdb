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

	flag.StringVar(&input, "i", "", "input: leveldb path list, separated by comma")
	flag.StringVar(&output, "o", "", "output: new leveldb path list, separated by comma")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -i [old leveldb path] -o [new leveldb path]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Keep old leveldb path resharding example: %s -i /data1,/newfolder1\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Resharding old leveldb to new path example: %s -i /data1 -o /newfolder1,/newfolder2\n", os.Args[0])
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
	if len(outputPath) == 0 {
		sdb, err := shardingdb.OpenFile(inputPathList, nil)
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
		sdb, err := shardingdb.OpenFile(outputPath, nil)
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
