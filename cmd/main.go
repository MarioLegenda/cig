package main

import (
	"fmt"
	"github.com/MarioLegenda/cig"
)

func main() {
	c := cig.New()

	res := c.Run("SELECT * FROM path:../testdata/example.csv AS e")

	fmt.Println(res.Errors())
}
