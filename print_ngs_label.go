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
  "log"
  "os/exec"
)

var (
  hostname   string
  num_labels int
)

func init() {
  flag.StringVar(&hostname, "host", "localhost", "Redis server hostname")
  flag.IntVar(&num_labels, "labels", 1, "Number of labels to print")

  flag.Parse()
}

func save_used_label(p *redis.Pool, label string) {
  redis.String(p.Get().Do("SADD", "ngs_ids_used_000", label))
}

func printLabel(p *redis.Pool, num_ids int) {

  lp := exec.Command("/usr/bin/lp")

  lp_in, err := lp.StdinPipe()
  if err != nil {
    panic(err)
  }
  err = lp.Start()
  if err != nil {
    panic(err)
  }

  for i := 0; i < num_ids; i++ {
    reply, err := redis.String(p.Get().Do("SPOP", "ngs_ids_000"))
    if err != nil {
      log.Fatal(err)
    }
    fmt.Printf("ngs-%s\n", reply)
    lp_in.Write([]byte(fmt.Sprintf("ngs-%s\n", reply)))
    go save_used_label(p, reply)
  }

  lp_in.Close()

  lp.Wait()
}

func main() {
  p := redis.NewPool(func() (redis.Conn, error) { return redis.Dial("tcp", fmt.Sprintf("%s:6379", hostname)) }, 2)
  defer p.Close()

  printLabel(p, num_labels)
}
