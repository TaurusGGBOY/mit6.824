package main

import "fmt"

func main() {
	a := make([]int, 0)
	a = append(a, 0)
	a = append(a, 1)
	a = append(a, 2)
	a = append(a, 3)
	a = append(a, 4)
	fmt.Println("hello %v", a[4:])
	fmt.Println("hello2 %v", a[5:])
	fmt.Println("hello2 %v", a[6:])
}
