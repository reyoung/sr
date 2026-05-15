package main

import (
	"context"
	"fmt"
	"os"

	"github.com/reyoung/sr/sr"
)

func main() {
	if err := sr.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
