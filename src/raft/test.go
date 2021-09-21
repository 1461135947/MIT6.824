/**
  @author:xzz
  @date:2021/9/18
  @File : test
  @note:
*/
package raft

import "sync"

func main()  {
	vec:=make([]int ,0)
	mu:=sync.Mutex{}

	cond1:=sync.NewCond(&mu)
	cond2:=sync.NewCond(&mu)
	//生产者
	go func() {
		for  {
			mu.Lock()
			 for len(vec)>=100{
				cond1.Wait()
			}
			vec=append(vec,1)
			mu.Unlock()
		}


	}()

	go func() {
		for  {
			mu.Lock()
			for len(vec)==0{
				cond2.Wait()
			}
			vec=vec[1:len(vec)]
			mu.Unlock()
		}
	}()
	select {

	}
}
