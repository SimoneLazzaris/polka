package main
import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)


var (
	rdb_pool *redis.Pool
)

func verify_reputation(sender string, ip string) (bool,string) {
	if _,ok:=cfg["reputation_redis_address"]; !ok {
		return true,""
	}
	xsender:=fmt.Sprintf("<%s>",sender)
	score:=reputation_score(xsender, ip)
	log_msg:=fmt.Sprintf(" reputation score %.3f",score)
	if score > 0.2 {
		if *xdebug {fmt.Printf("Accepting sender: %s:%s score %f\n", sender, ip, score)}
		return true, log_msg
	}
	xlog.Info(fmt.Sprintf("Rejecting bad sender: %s:%s score %f ", sender, ip, score))
	return false, log_msg
}

func reputation_score(sender string, ip string) float64 {
	if *xdebug { fmt.Printf("REPUTATION: Sender %s from ip %s\n",sender,ip) }
	rdb:=rdb_pool.Get()
	defer rdb.Close()
	rep,err:=redis.Float64(rdb.Do("GET", fmt.Sprintf("TRACK:%s:%s",sender,ip)))
	if err!=nil {
		rep,err=redis.Float64(rdb.Do("GET", fmt.Sprintf("TRACK:IP:%s",ip)))
	}
	if err!=nil {
		log.Println(err)
		return 1.0
	}
	return rep
		
}

func InitReputation() {
	if addr,ok:=cfg["reputation_redis_address"]; ok {
		if *xdebug {fmt.Println("Initializing pool")}
		rdb_pool=&redis.Pool {
			MaxIdle: 10,
			IdleTimeout: 120*time.Second,
			Dial: func() (redis.Conn, error) {
				if *xdebug {fmt.Println("Connecting to REDIS "+addr)}
				return redis.Dial("tcp", addr)
			},
		}
	}
}

