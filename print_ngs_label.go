package main

import (
  "github.com/garyburd/redigo/redis"
  "os/exec"
  "fmt"
  "log"
  "flag"
)

var (
  hostname string
  num_labels int
)

func init() {
  flag.StringVar(&hostname,"host","localhost","Redis server hostname")
  flag.IntVar(&num_labels,"labels",1,"Number of labels to print")

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

  for i:=0; i < num_ids; i++ {
    reply, err := redis.String(p.Get().Do("SPOP","ngs_ids_000"))
    if err != nil {
      log.Fatal(err)
    }
    fmt.Printf("ngs-%s\n",reply)
    lp_in.Write([]byte(fmt.Sprintf("ngs-%s\n",reply)))
    go save_used_label(p,reply)
  }

  lp_in.Close()

  lp.Wait()
}

func main() {
  p := redis.NewPool(func() (redis.Conn, error) { return redis.Dial("tcp", fmt.Sprintf("%s:6379",hostname)) }, 2)
  defer p.Close()

  printLabel(p,num_labels)
}
