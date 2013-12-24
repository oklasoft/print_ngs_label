package main

import "github.com/garyburd/redigo/redis"
import "os"
import "os/exec"
import "fmt"
import "log"

func printLabel(r redis.Conn, num_ids int) {

  lp := exec.Command("/bin/cat")
  outfile, err := os.Create("./out.txt")
  if err != nil {
      panic(err)
  }
  defer outfile.Close()
  lp.Stdout = outfile

  lp_in, err := lp.StdinPipe()
  if err != nil {
    panic(err)
  }
  err = lp.Start()
  if err != nil {
      panic(err)
  }
  
  for i:=0; i < num_ids; i++ {
    reply, err := redis.String(r.Do("srandmember","ngs_ids_000"))
    if err != nil {
      log.Fatal(err)
    }
    fmt.Printf("ngs-%s\n",reply)
    lp_in.Write([]byte(fmt.Sprintf("ngs-%s\n",reply)))
  }

  lp_in.Close()

  lp.Wait()
}

func main() {
  r, err := redis.Dial("tcp", "redis:6379")
  if nil != err {
    log.Fatal(err)
  }
  defer r.Close()

  printLabel(r,1)
}
