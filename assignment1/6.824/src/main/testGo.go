package main

import "time"

func main() {
	go func() {
		for{
			print("test\n")
		}
	}()
	time.Sleep(time.Duration(10)*time.Millisecond)
}
