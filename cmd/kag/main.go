package main

import (
	"time"

	"github.com/savaki/kag"
)

func main() {
	monitor := kag.New(kag.Config{})
	defer monitor.Close()

	time.Sleep(10 * time.Second)
}
