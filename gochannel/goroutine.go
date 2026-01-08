package main

import "fmt"

func rust(channel <-chan int) {

	for value := range channel {

		fmt.Println("Received value:", value)
	}

}
