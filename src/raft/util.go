package raft

import (

	"log"
	"math/rand"
	"time"
)


// Debugging
const Debug =0

var heartBeatTime=50*time.Millisecond

func getRandomElectTime() time.Duration {
	randTime:=rand.Int63n(150)+150

	return time.Millisecond *time.Duration(randTime)
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func ReStartTimer(timer *time.Timer,duration time.Duration)  {
	timer.Stop()
	timer.Reset(duration)
}
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func init()  {
	log.SetFlags(log.Lmicroseconds)
}