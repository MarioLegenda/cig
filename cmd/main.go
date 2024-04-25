package main

import (
	"fmt"
	"github.com/MarioLegenda/cig"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var rootCmd = &cobra.Command{
	Use:   "cig",
	Short: "cig allows you to query CSV file with SQL syntax",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := cig.New()

		result, err := c.Run(strings.Join(args, ""))
		if err != nil {
			return err
		}

		if len(result) == 0 {
			fmt.Println("")
			fmt.Println("Empty result returned. Nothing found.")
			fmt.Println("")
			return nil
		}

		keys := make(table.Row, 0)
		rows := make([]table.Row, 0)
		for _, res := range result {
			if len(keys) == 0 {
				for key, _ := range res {
					keys = append(keys, key)
				}

				continue
			}

			values := make(table.Row, len(res))
			i := 0
			for _, key := range keys {
				val := res[key.(string)]
				values[i] = val
				i++
			}

			rows = append(rows, values)
		}

		t := table.NewWriter()
		t.SetPageSize(100)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(keys)
		t.AppendRows(rows)
		t.Render()

		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
