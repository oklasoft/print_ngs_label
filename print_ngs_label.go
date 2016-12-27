// Simple app to grab ID from set and print it
//
// Copyright 2013 Stuart Glenn, Oklahoma Medical Research Founation. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"log"
	"os/exec"
	"sync"
	"time"
)

const Offset = 1480550400
const Prefix = "cgc"

var (
	hostname      string
	num_labels    int
	num_copies    int
	skip_print    bool
	make_clinical bool
	verbose       bool
	wg            sync.WaitGroup
)

func init() {
	flag.StringVar(&hostname, "host", "localhost", "Redis server hostname")
	flag.IntVar(&num_labels, "labels", 1, "Number of labels to print")
	flag.IntVar(&num_copies, "copies", 1, "Number of copies to print")
	flag.BoolVar(&skip_print, "skip-print", false, "Skip the actual printing")
	flag.BoolVar(&make_clinical, "make-clinical", false, "For each ngs label also make a clinical label")
	flag.BoolVar(&verbose, "verbose", false, "Print IDs to STDOUT")

	flag.Parse()
}

func generateId(p *redis.Pool) string {
	var id string
	for r := 0; r < 5; r++ {
		t := int64(time.Now().UTC().Unix()) - Offset
		for i := 0; i < 98; i++ {
			id = fmt.Sprintf("%s%x%02d", Prefix, t, i+1)
			exists, err := redis.Bool(p.Get().Do("SISMEMBER", "cgc-ids", id))
			if err != nil {
				log.Fatal(err)
			}
			if !exists {
				break
			} else {
				id = ""
			}
		}
		if "" != id {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	wg.Add(1)
	go saveCgcId(p, id)
	return id
}

func saveCgcId(p *redis.Pool, id string) {
	defer wg.Done()
	redis.String(p.Get().Do("SADD", "cgc-ids", id))
}

func save_used_label(p *redis.Pool, label string) {
	defer wg.Done()
	redis.String(p.Get().Do("SADD", "ngs_ids_used_000", label))
}

func printLabel(p *redis.Pool, num_ids int, num_copies int) {

	var lp *exec.Cmd
	var lp_in io.WriteCloser

	if !skip_print {
		lp = exec.Command("/usr/bin/lp")

		var err error
		lp_in, err = lp.StdinPipe()
		if err != nil {
			panic(err)
		}
		err = lp.Start()
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < num_ids; i++ {
		reply, err := redis.String(p.Get().Do("SPOP", "ngs_ids_000"))
		if err != nil {
			log.Fatal(err)
		}
		var clinId string
		if make_clinical {
			clinId = generateId(p)
		}
		if verbose {
			fmt.Printf("ngs-%s\n", reply)
			if make_clinical {
				fmt.Printf("%s\n", clinId)
			}
		}
		if !skip_print {
			for c := 0; c < num_copies; c++ {
				lp_in.Write([]byte(fmt.Sprintf("ngs-%s\n", reply)))
				if make_clinical {
					lp_in.Write([]byte(fmt.Sprintf("%s\n", clinId)))
				}
			}
		}
		wg.Add(1)
		go save_used_label(p, reply)
	}

	if !skip_print {
		lp_in.Close()

		lp.Wait()
	}
}

func main() {
	p := redis.NewPool(func() (redis.Conn, error) { return redis.Dial("tcp", fmt.Sprintf("%s:6379", hostname)) }, 2)
	defer p.Close()

	printLabel(p, num_labels, num_copies)
	wg.Wait()
}
